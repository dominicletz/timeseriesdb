defmodule TimeSeriesDB.State do
  @chunk_size 5_000_000
  @max_files_default 100
  alias TimeSeriesDB.State

  defstruct [
    :first_timestamp,
    :last_timestamp,
    :dirname,
    :timestamps,
    :size,
    :max_files
  ]

  defp compress(data), do: :ezstd.compress(data)
  defp decompress(data), do: :ezstd.decompress(data)

  def init(dirname, opts \\ []) do
    max_files = Keyword.get(opts, :max_files, @max_files_default)
    state = %State{dirname: dirname, max_files: max_files}
    File.mkdir_p!(dirname)

    state =
      case File.stat(current(state)) do
        {:ok, %{size: size}} when size > 0 ->
          timestamps = read_file(current(state))

          if timestamps == [] do
            %State{state | timestamps: [], size: 0}
          else
            first_timestamp = timestamps |> List.first() |> elem(0)
            last_timestamp = timestamps |> List.last() |> elem(0)

            %State{
              state
              | first_timestamp: first_timestamp,
                last_timestamp: last_timestamp,
                timestamps: timestamps,
                size: size
            }
          end

        _ ->
          last_timestamp =
            with {_first_timestamp, last_timestamp, _filename, _} <-
                   List.first(list_all_logs(state)) do
              last_timestamp
            end

          %State{state | timestamps: [], size: 0, last_timestamp: last_timestamp}
      end

    {:ok, state}
  end

  defp current(state) do
    Path.join(state.dirname, "v1_current.bin")
  end

  defp range_prefix(state) do
    Path.join(state.dirname, "v1_range_")
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

  def append_row!(state, timestamp \\ nil, row) do
    case append_row(state, timestamp, row) do
      {:ok, state} -> state
      {:error, reason} -> raise reason
    end
  end

  def append_row(state, timestamp \\ nil, row) do
    timestamp = timestamp || System.os_time(:nanosecond)

    if state.last_timestamp != nil and state.last_timestamp > timestamp do
      {:error,
       "Timestamp #{inspect(timestamp)} is not monotonic! Must be greater than #{inspect(state.last_timestamp)}"}
    else
      state = rotate_if_needed(state)
      est_size = byte_size(:erlang.term_to_binary(row)) + 20

      {:ok,
       %{
         state
         | first_timestamp: state.first_timestamp || timestamp,
           last_timestamp: timestamp,
           timestamps: [{timestamp, row} | state.timestamps],
           size: state.size + est_size
       }}
    end
  end

  def query_range(state, from, to) when from <= to do
    list_all_logs(state)
    |> Enum.reverse()
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
    |> Enum.map(fn {first_timestamp, last_timestamp, filename, open_fn} ->
      %{
        filename: filename,
        count: length(open_fn.()),
        first_timestamp: first_timestamp,
        last_timestamp: last_timestamp
      }
    end)
  end

  defp rotate_if_needed(state) do
    if state.size > @chunk_size do
      rotate_log(state)
    else
      state
    end
  end

  def flush(state) do
    File.write!(current(state), :erlang.term_to_binary(Enum.reverse(state.timestamps)))
    state
  end

  defp read_file(filename) do
    :erlang.binary_to_term(File.read!(filename))
  end

  defp uint64_str(num) do
    String.pad_leading("#{num}", 16, "0")
  end

  defp rotate_log(state) do
    base_name =
      range_prefix(state) <>
        "#{uint64_str(state.first_timestamp)}-#{uint64_str(state.last_timestamp)}"

    data =
      Enum.reverse(state.timestamps)
      |> :erlang.term_to_binary()
      |> compress()

    File.write!(base_name <> ".zst", data)
    delete_old_logs(state)

    %{state | first_timestamp: nil, last_timestamp: nil, size: 0, timestamps: []}
  end

  defp delete_old_logs(state) do
    prefix = Path.basename(range_prefix(state))

    File.ls!(state.dirname)
    |> Enum.sort(:desc)
    |> Enum.filter(&String.starts_with?(&1, prefix))
    |> Enum.drop(state.max_files)
    |> Enum.each(&File.rm/1)
  end

  defp list_logs(state) do
    prefix = Path.basename(range_prefix(state))

    File.ls!(state.dirname)
    |> Enum.sort(:desc)
    |> Enum.filter(fn filename -> String.starts_with?(filename, prefix) end)
    |> Enum.map(fn filename ->
      [first_timestamp, last_timestamp] =
        String.replace_prefix(filename, prefix, "")
        |> Path.basename(Path.extname(filename))
        |> String.split("-")

      {String.to_integer(first_timestamp), String.to_integer(last_timestamp), filename,
       fn ->
         Path.join(state.dirname, filename)
         |> File.read!()
         |> decompress()
         |> :erlang.binary_to_term()
       end}
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
end
