defmodule TimeSeriesDB do
  @moduledoc """
  A time series database that stores data in a directory of files.

  Example:

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db", max_files: 2)
  iex> TimeSeriesDB.append_row(pid, 1, %{x: 1, y: 2, z: "hello"})
  iex> TimeSeriesDB.append_row(pid, 2, %{x: 2, y: 3, z: "world"})
  iex> TimeSeriesDB.append_row(pid, 3, %{x: 3, y: 4, z: "elixir"})
  iex> TimeSeriesDB.query_range(pid, 1, 2)
  [{1, %{x: 1, y: 2, z: "hello"}}, {2, %{x: 2, y: 3, z: "world"}}]
  iex> TimeSeriesDB.query_multiple(pid, [3, 2])
  [%{x: 3, y: 4, z: "elixir"}, %{x: 2, y: 3, z: "world"}]
  ```
  """

  require Logger
  alias TimeSeriesDB.State
  use GenServer

  @doc """
  Starts the server.

  ## Options

  - `:max_files` - The maximum number of files to keep in the database. Defaults to `100`.
  - `:name` - The name of the process.

  ## Example:

  ```elixir
  iex> {:ok, _pid} = TimeSeriesDB.start_link("testdata/test_db", max_files: 2)
  ```
  """
  def start_link(filename, opts \\ []) do
    {opts, gen_opts} = Keyword.split(opts, [:max_files])
    GenServer.start_link(__MODULE__, {filename, opts}, gen_opts)
  end

  @doc false
  def init({filename, opts}), do: State.init(filename, opts)

  @doc """
  Appends a row to the database.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db", max_files: 2)
  iex> TimeSeriesDB.append_row(pid, 1, %{x: 1, y: 2, z: "hello"})
  :ok
  ```
  """
  def append_row(pid, key, value) when is_integer(key) do
    GenServer.cast(pid, {:append_row, key, value})
  end

  @doc """
  Appends a row to the database. Same as `append_row(pid, value)`, but with a nanosecond timestamp as default key.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db", max_files: 2)
  iex> TimeSeriesDB.append_row(pid, %{x: 1, y: 2, z: "hello"})
  :ok
  ```
  """
  def append_row(pid, value) do
    key = System.os_time(:nanosecond)
    GenServer.cast(pid, {:append_row, key, value})
  end

  @doc """
  Queries a range of rows from the database.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db", max_files: 2)
  iex> TimeSeriesDB.append_row(pid, 1, %{x: 1, y: 2, z: "hello"})
  iex> TimeSeriesDB.append_row(pid, 2, %{x: 2, y: 3, z: "world"})
  iex> TimeSeriesDB.query_range(pid, 1, 2)
  [{1, %{x: 1, y: 2, z: "hello"}}, {2, %{x: 2, y: 3, z: "world"}}]
  ```
  """
  def query_range(pid, from, to) when from <= to,
    do: GenServer.call(pid, {:query_range, from, to})

  @doc """
  Queries multiple rows from the database.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db")
  iex> TimeSeriesDB.append_row(pid, 1, %{x: 1, y: 2, z: "hello"})
  iex> TimeSeriesDB.append_row(pid, 2, %{x: 2, y: 3, z: "world"})
  iex> TimeSeriesDB.query_multiple(pid, [3, 2, 1])
  [nil, %{x: 2, y: 3, z: "world"}, %{x: 1, y: 2, z: "hello"}]
  ```
  """
  def query_multiple(pid, keys), do: GenServer.call(pid, {:query_multiple, keys})

  @doc """
  Counts the number of rows in the database.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db")
  iex> TimeSeriesDB.append_row(pid, 1, %{x: 1, y: 2, z: "hello"})
  iex> TimeSeriesDB.append_row(pid, 2, %{x: 2, y: 3, z: "world"})
  iex> TimeSeriesDB.count(pid)
  2
  ```
  """
  def count(pid), do: GenServer.call(pid, :count)

  @doc """
  Counts the number of files in the database.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db")
  iex> for x <- 1..20_000, do: TimeSeriesDB.append_row(pid, x, %{x: x, z: String.duplicate("hello", 100)})
  iex> TimeSeriesDB.flush(pid)
  iex> TimeSeriesDB.count_files(pid) |> length()
  3
  ```
  """
  def count_files(pid), do: GenServer.call(pid, :count_files)

  @doc """
  Returns    the oldest timestamp in the database.
  """
  def oldest(pid), do: GenServer.call(pid, :oldest)

  @doc """
  Returns the newest timestamp in the database.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db")
  iex> TimeSeriesDB.append_row(pid, 1, %{x: 1, y: 2, z: "hello"})
  iex> TimeSeriesDB.append_row(pid, 2, %{x: 2, y: 3, z: "world"})
  iex> TimeSeriesDB.newest(pid)
  2
  ```
  """
  def newest(pid), do: GenServer.call(pid, :newest)

  @doc """
  Flushes the database to disk.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db")
  iex> TimeSeriesDB.flush(pid)
  :ok
  ```
  """
  def flush(pid), do: GenServer.call(pid, :flush)

  @doc """
  Stops the server.

  ## Examples

  ```elixir
  iex> {:ok, pid} = TimeSeriesDB.start_link("testdata/test_db")
  iex> TimeSeriesDB.stop(pid)
  :ok
  ```
  """
  def stop(pid), do: GenServer.stop(pid)

  def handle_cast({:append_row, key, value}, state) do
    case State.append_row(state, key, value) do
      {:ok, state} ->
        {:noreply, state}

      {:error, reason} ->
        Logger.error("Error appending row: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_call({:query_range, from, to}, _from, state) do
    {:reply, State.query_range(state, from, to), state}
  end

  def handle_call({:query_multiple, keys}, _from, state) do
    {:reply, State.query_multiple(state, keys), state}
  end

  def handle_call(:flush, _from, state) do
    {:reply, :ok, State.flush(state)}
  end

  def handle_call(:count, _from, state), do: {:reply, State.count(state), state}
  def handle_call(:count_files, _from, state), do: {:reply, State.count_files(state), state}
  def handle_call(:oldest, _from, state), do: {:reply, State.oldest(state), state}
  def handle_call(:newest, _from, state), do: {:reply, State.newest(state), state}

  def terminate(_reason, state) do
    State.flush(state)
  end
end
