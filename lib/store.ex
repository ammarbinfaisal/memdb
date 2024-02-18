defmodule KeyValue do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    {:ok, %{data: %{}, slaves: []}}
  end

  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state.data, key), state}
  end

  def handle_call({:set, key, value}, _from, state) do
    {:reply, :ok, %{state | data: Map.put(state.data, key, value)}}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, Map.get(state.data, key), %{state | data: Map.delete(state.data, key)}}
  end

  def handle_call({:sync, data}, _from, state) do
    {:reply, :ok, %{state | data: data}}
  end

  def handle_call({:add_slave, pid}, _from, state) do
    {:reply, :ok, %{state | slaves: [pid | state.slaves |> Enum.uniq]}}
  end

  def handle_call({:get_slaves}, _from, state) do
    {:reply, state.slaves, state}
  end
end
