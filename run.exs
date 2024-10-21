#!/usr/bin/env elixir

Mix.install([:profiler])

defmodule TimeSeriesDB do
  @max_size 10_000_000
  @max_files 100

  defstruct [:cmd, :first_timestamp, :last_timestamp, :fp, :filename, :timestamps]

  @compress_cmds [
                   {"zstd", ["--rm"], ".zst"},
                   {"xz", [], ".xz"},
                   {"gzip", [], ".gz"}
                 ]
                 |> Enum.map(fn {cmd, args, ext} -> %{cmd: cmd, args: args, ext: ext} end)

  def init(filename, _opts \\ []) do
    {first_timestamp, last_timestamp, timestamps} =
      if File.exists?(filename) do
        {:ok, fp} = :file.open(filename, [:read, :binary])
        timestamps = read_timestamps(fp)
        :file.close(fp)

        if timestamps == [] do
          {nil, nil, []}
        else
          first_timestamp = timestamps |> List.first() |> elem(0)
          last_timestamp = timestamps |> List.last() |> elem(0)
          {first_timestamp, last_timestamp, timestamps}
        end
      else
        {nil, nil, []}
      end

    {:ok, fp} = :file.open(filename, [:append, :write, :binary, :delayed_write])

    {:ok,
     %TimeSeriesDB{
       cmd:
         Enum.find(@compress_cmds, fn cmd -> System.find_executable(cmd.cmd) end) ||
           raise("No compression tool found"),
       first_timestamp: first_timestamp,
       last_timestamp: last_timestamp,
       fp: fp,
       filename: filename,
       timestamps: Enum.reverse(timestamps)
     }}
  end

  def close(%TimeSeriesDB{fp: fp}) do
    :file.close(fp)
  end

  def oldest(state) do
    with {first_timestamp, _last_timestamp, _filename, _} <- List.last(list_all_logs(state)) do
      first_timestamp
    end
  end

  def newest(state) do
    if state.last_timestamp != nil do
      state.last_timestamp
    end
  end

  def append_row(state, timestamp \\ nil, row) do
    state = rotate_if_needed(state)

    timestamp =
      case timestamp do
        nil ->
          System.os_time(:nanosecond)

        ts ->
          if state.last_timestamp != nil and state.last_timestamp > ts do
            raise(
              "Timestamp #{inspect(ts)} is not monotonic! Must be greater than #{inspect(state.last_timestamp)}"
            )
          end

          ts
      end

    data = :erlang.term_to_binary(row)
    size = byte_size(data)

    pos = :file.position(state.fp, :cur)
    :file.write(state.fp, <<timestamp::unsigned-size(64), size::unsigned-size(32), data::binary>>)

    %{
      state
      | first_timestamp: state.first_timestamp || timestamp,
        last_timestamp: timestamp,
        timestamps: [{timestamp, {pos + 12, size}} | state.timestamps]
    }
  end

  def query_range(state, from, to) do
    list_all_logs(state)
    |> Enum.filter(fn {first_timestamp, last_timestamp, _filename, _} ->
      first_timestamp <= to and from <= last_timestamp
    end)
    |> Enum.flat_map(fn {_first_timestamp, _last_timestamp, filename, open_fn} ->
      {:ok, fp} = open_fn.()
      timestamps = maybe_read_timestamps(state, filename, fp)

      ret =
        timestamps
        |> Enum.drop_while(fn {timestamp, _} -> timestamp < from end)
        |> Enum.reduce_while([], fn {timestamp, {loc, size}}, acc ->
          if timestamp <= to do
            {:ok, data} = :file.pread(fp, loc, size)
            {:cont, [{timestamp, :erlang.binary_to_term(data)} | acc]}
          else
            {:halt, acc}
          end
        end)

      :file.close(fp)
      Enum.reverse(ret)
    end)
  end

  def query_multiple(state, timestamps) do
    logs = list_all_logs(state)

    ret =
      Enum.reduce(timestamps, %{}, fn t, acc ->
        log =
          Enum.find(logs, fn {first_timestamp, last_timestamp, _filename, _} ->
            first_timestamp <= t and t <= last_timestamp
          end)

        if log != nil do
          Map.update(acc, log, [t], fn keys -> [t | keys] end)
        else
          acc
        end
      end)
      |> Enum.flat_map(fn {{_first_timestamp, _last_timestamp, _filename, open_fn}, keys} ->
        {:ok, fp} = open_fn.()
        timestamps = read_timestamps(fp) |> Map.new()

        ret =
          Enum.map(keys, fn key ->
            with {loc, size} <- Map.get(timestamps, key) do
              {:ok, data} = :file.pread(fp, loc, size)
              {key, :erlang.binary_to_term(data)}
            end
          end)

        :file.close(fp)
        ret
      end)
      |> Map.new()

    Enum.map(timestamps, fn timestamp ->
      Map.get(ret, timestamp, nil)
    end)
  end

  def count(state) do
    list_all_logs(state)
    |> Enum.map(fn {_first_timestamp, _last_timestamp, _filename, open_fn} ->
      {:ok, fp} = open_fn.()
      timestamps = read_timestamps(fp, 0)
      :file.close(fp)
      length(timestamps)
    end)
    |> Enum.sum()
  end

  def count_files(state) do
    list_all_logs(state)
    |> Enum.map(fn {_first_timestamp, _last_timestamp, filename, open_fn} ->
      {:ok, fp} = open_fn.()
      timestamps = read_timestamps(fp, 0)
      :file.close(fp)
      {filename, length(timestamps)}
    end)
  end

  defp rotate_if_needed(state) do
    with {:ok, size} when size > @max_size <- :file.position(state.fp, :cur) do
      rotate_log(state)
    else
      _ -> state
    end
  end

  defp rotate_log(state) do
    :file.close(state.fp)
    compressed_name = "#{state.filename}.#{state.first_timestamp}-#{state.last_timestamp}"
    File.rename(state.filename, compressed_name)

    spawn(fn ->
      System.cmd(state.cmd.cmd, state.cmd.args ++ [compressed_name])
      delete_old_logs(state)
    end)

    {:ok, fp} = :file.open(state.filename, [:write, :binary, :delayed_write])
    %{state | fp: fp, first_timestamp: nil, last_timestamp: nil}
  end

  defp read_timestamps(fp) do
    read_timestamps(fp, 0)
  end

  defp read_timestamps(fp, loc) do
    case :file.pread(fp, loc, 12) do
      {:ok, <<timestamp::unsigned-size(64), size::unsigned-size(32)>>} ->
        [{timestamp, {loc + 12, size}} | read_timestamps(fp, loc + 12 + size)]

      {:ok, ""} ->
        []

      :eof ->
        []
    end
  end

  defp read_blob(<<timestamp::unsigned-size(64), size::unsigned-size(32), bin::binary-size(size), rest::binary()>>) do
    [{timestamp, :erlang.binary_to_term(bin)} | read_blob(rest)]
  end

  defp delete_old_logs(state) do
    File.ls!()
    |> Enum.sort(:desc)
    |> Enum.filter(&String.starts_with?(&1, state.filename <> "."))
    |> Enum.drop(@max_files)
    |> Enum.each(&File.rm/1)
  end

  defp list_logs(state) do
    prefix = state.filename <> "."

    File.ls!()
    |> Enum.sort(:desc)
    |> Enum.filter(fn filename ->
      String.starts_with?(filename, prefix) and Path.extname(filename) != ".timestamps"
    end)
    |> Enum.map(fn filename ->
      [first_timestamp, last_timestamp] =
        String.replace_prefix(filename, state.filename <> ".", "")
        |> Path.basename(Path.extname(filename))
        |> String.split("-")

      {String.to_integer(first_timestamp), String.to_integer(last_timestamp), filename,
       fn -> decompress(state, filename) end}
    end)
  end

  defp list_all_logs(state) do
    if state.first_timestamp != nil do
      now =
        {state.first_timestamp, state.last_timestamp, :state,
         fn -> :file.open(state.filename, [:read, :binary]) end}

      [now | list_logs(state)]
    else
      list_logs(state)
    end
  end

  defp decompress(_state, filename) do
    ext = Path.extname(filename)

    cmd =
      Enum.find(@compress_cmds, fn cmd -> cmd.ext == ext end) ||
        raise("No decompressor found for #{inspect(filename)}")

    {file, 0} = System.cmd(cmd.cmd, ["-dc", filename])
    :file.open(file, [:read, :binary, :ram])
  end

  defp maybe_read_timestamps(state, :state, _fp, _loc) do
    Enum.reverse(state.timestamps)
  end

  defp maybe_read_timestamps(_state, filename, fp, loc) do
    read_timestamps(fp, loc)

    # if File.exists?(filename <> ".timestamps") do
    #   File.read!(filename <> ".timestamps") |> :erlang.binary_to_term()
    # else
    #   tps = read_timestamps(fp, loc)
    #   File.write!(filename <> ".timestamps", :erlang.term_to_binary(tps, [:compressed]))
    #   tps
    # end
  end
end

exists? = File.exists?("data.bin")
{:ok, state} = TimeSeriesDB.init("data.bin")

make_element = fn x ->
  y = :erlang.phash2(x)
  %{x: x, y: :erlang.crc32("#{y}"), z: String.duplicate("#{y}", 100)}
end

state =
  if not exists? do
    Enum.reduce(1..100_000, state, fn x, state ->
      TimeSeriesDB.append_row(state, x, make_element.(x))
    end)
  else
    state
  end

# TimeSeriesDB.close(state)

1 = TimeSeriesDB.oldest(state)
100_000 = TimeSeriesDB.newest(state)
100_000 = TimeSeriesDB.count(state)

[{1, one}] = TimeSeriesDB.query_range(state, 1, 1)
[^one] = TimeSeriesDB.query_multiple(state, [1])
[{2, _two}] = TimeSeriesDB.query_range(state, 2, 2)

[{100_000, one}] = TimeSeriesDB.query_range(state, 100_000, 100_000)
[^one] = TimeSeriesDB.query_multiple(state, [100_000])

# IO.inspect(TimeSeriesDB.count_files(state))
:timer.tc(fn -> TimeSeriesDB.query_range(state, 90_000, 95_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 90_000, 95_000) end) |> elem(0) |> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_range(state, 33_000, 38_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 33_000, 38_000) end) |> elem(0) |> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_multiple(state, [1, 10, 100, 1000, 10_000, 100_000]) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_multiple(state, [1, 10, 100, 1000, 10_000, 100_000]) end) |> elem(0) |> IO.inspect()

# Profiler.fprof(fn -> TimeSeriesDB.query_range(state, 90_000, 95_000) end)
# Profiler.fprof(fn -> TimeSeriesDB.query_multiple(state, [1, 10, 100, 1000, 10_000, 100_000]) end)
