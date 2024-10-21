#!/usr/bin/env elixir

Mix.install([:profiler])

defmodule TimeSeriesDB do
  @max_size 10_000_000
  @max_files 100

  defstruct [:cmd, :first_timestamp, :last_timestamp, :fp, :filename, :timestamps, :size]

  @compress_cmds [
                   {"zstd", ["--rm"], ".zst"},
                   {"xz", [], ".xz"},
                   {"gzip", [], ".gz"}
                 ]
                 |> Enum.map(fn {cmd, args, ext} -> %{cmd: cmd, args: args, ext: ext} end)

  def init(filename, _opts \\ []) do
    {first_timestamp, last_timestamp, timestamps, size} =
      case File.stat(filename) do
        {:ok, %{size: size}} when size > 0 ->
          timestamps = read_file(filename)

          if timestamps == [] do
            {nil, nil, [], 0}
          else
            first_timestamp = timestamps |> List.first() |> elem(0)
            last_timestamp = timestamps |> List.last() |> elem(0)
            {first_timestamp, last_timestamp, timestamps, size}
          end

        _ ->
          {nil, nil, [], 0}
      end

    cmd = Enum.find_value(@compress_cmds, fn cmd ->
      if full_path = System.find_executable(cmd.cmd) do
        %{cmd | cmd: full_path}
      end
    end) || raise("No compression tool found")

    {:ok,
     %TimeSeriesDB{
       cmd: cmd,
       first_timestamp: first_timestamp,
       last_timestamp: last_timestamp,
       filename: filename,
       size: size,
       timestamps: Enum.reverse(timestamps)
     }}
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

    est_size = byte_size(:erlang.term_to_binary(row)) + 20

    %{
      state
      | first_timestamp: state.first_timestamp || timestamp,
        last_timestamp: timestamp,
        timestamps: [{timestamp, row} | state.timestamps],
        size: state.size + est_size
    }
  end

  def query_range(state, from, to) do
    list_all_logs(state)
    |> Enum.filter(fn {first_timestamp, last_timestamp, _filename, _} ->
      first_timestamp <= to and from <= last_timestamp
    end)
    |> Enum.flat_map(fn {_first_timestamp, _last_timestamp, _filename, open_fn} ->
      open_fn.()
      |> Enum.filter(fn {timestamp, _} -> from <= timestamp and timestamp <= to end)
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
        timestamps = open_fn.() |> Map.new()
        Enum.map(keys, fn key -> {key, Map.get(timestamps, key)} end)
      end)
      |> Map.new()

    Enum.map(timestamps, fn timestamp ->
      Map.get(ret, timestamp, nil)
    end)
  end

  def count(state) do
    list_all_logs(state)
    |> Enum.map(fn {_first_timestamp, _last_timestamp, _filename, open_fn} ->
      length(open_fn.())
    end)
    |> Enum.sum()
  end

  def count_files(state) do
    list_all_logs(state)
    |> Enum.map(fn {_first_timestamp, _last_timestamp, filename, open_fn} ->
      {filename, length(open_fn.())}
    end)
  end

  defp rotate_if_needed(state) do
    if state.size > @max_size do
      rotate_log(state)
    else
      state
    end
  end

  def flush(state) do
    File.write!(state.filename, :erlang.term_to_binary(Enum.reverse(state.timestamps)))
  end

  defp read_file(filename) do
    :erlang.binary_to_term(File.read!(filename))
  end

  defp rotate_log(state) do
    flush(state)
    base_name = "#{state.filename}.#{state.first_timestamp}-#{state.last_timestamp}"
    raw_name = base_name <> ".raw"
    File.rename(state.filename, raw_name)

    spawn(fn ->
      random = Base.encode32(:rand.bytes(10), case: :lower)
      tmpnam = "tmp.#{random}"
      compressor = state.cmd
      System.cmd(compressor.cmd, compressor.args ++ ["-o", tmpnam, raw_name])
      File.rename(tmpnam, base_name <> compressor.ext)
      File.rm(raw_name)
      delete_old_logs(state)
    end)

    %{state | first_timestamp: nil, last_timestamp: nil, size: 0, timestamps: []}
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
         fn -> Enum.reverse(state.timestamps) end}

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
    :erlang.binary_to_term(file)
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

TimeSeriesDB.flush(state)

1 = TimeSeriesDB.oldest(state)
100_000 = TimeSeriesDB.newest(state)
100_000 = TimeSeriesDB.count(state)

[] = TimeSeriesDB.query_range(state, 0, 0)
[{1, one}] = TimeSeriesDB.query_range(state, 0, 1)
[{1, ^one}] = TimeSeriesDB.query_range(state, 1, 1)
[{1, ^one}] = TimeSeriesDB.query_range(state, 0.1, 1.9)

[^one] = TimeSeriesDB.query_multiple(state, [1])
[{2, _two}] = TimeSeriesDB.query_range(state, 2, 2)

range = [1, 10, 100, 1000, 10_000, 100_000]
test = Enum.map(range, make_element)
^test = TimeSeriesDB.query_multiple(state, range)
[nil, nil, nil | ^test] = TimeSeriesDB.query_multiple(state, [1.5, 10.1, 100.2 | range])

[{100_000, one}] = TimeSeriesDB.query_range(state, 100_000, 100_000)
[^one] = TimeSeriesDB.query_multiple(state, [100_000])

# IO.inspect(TimeSeriesDB.count_files(state))
:timer.tc(fn -> TimeSeriesDB.query_range(state, 90_000, 95_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 90_000, 95_000) end) |> elem(0) |> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end) |> elem(0) |> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_range(state, 33_000, 38_000) end) |> elem(0) |> IO.inspect()
:timer.tc(fn -> TimeSeriesDB.query_range(state, 33_000, 38_000) end) |> elem(0) |> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_multiple(state, [1, 10, 100, 1000, 10_000, 100_000]) end)
|> elem(0)
|> IO.inspect()

:timer.tc(fn -> TimeSeriesDB.query_multiple(state, [1, 10, 100, 1000, 10_000, 100_000]) end)
|> elem(0)
|> IO.inspect()

# Profiler.fprof(fn -> TimeSeriesDB.query_range(state, 80_000, 85_000) end)
Profiler.fprof(fn -> TimeSeriesDB.query_multiple(state, [1, 10, 100, 1000, 10_000, 100_000]) end)
