defmodule TimeSeriesDB do
  alias TimeSeriesDB.State

  use GenServer

  def start_link(filename, opts \\ []) do
    GenServer.start_link(__MODULE__, filename, opts)
  end

  def init(filename), do: State.init(filename)
  def append_row(pid, key, value), do: GenServer.cast(pid, {:append_row, key, value})
  def query_range(pid, from, to), do: GenServer.call(pid, {:query_range, from, to})
  def query_multiple(pid, keys), do: GenServer.call(pid, {:query_multiple, keys})
  def count(pid), do: GenServer.call(pid, :count)
  def count_files(pid), do: GenServer.call(pid, :count_files)
  def oldest(pid), do: GenServer.call(pid, :oldest)
  def newest(pid), do: GenServer.call(pid, :newest)
  def flush(pid), do: GenServer.call(pid, :flush)
  def stop(pid), do: GenServer.stop(pid)

  def handle_cast({:append_row, key, value}, state) do
    {:noreply, State.append_row(state, key, value)}
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
