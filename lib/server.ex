defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  def handle_client(client, p) do
    IO.puts "Client connected"
    loop(client, p)
  end

  def parse_array(data, count, acc \\ []) do
    case count do
      0 -> {{:array, acc}, data}
      _ ->
        {parsed, rest} = parse(data)
        parse_array(rest, count - 1, acc ++ [parsed])
    end
  end

  def parse_bulk(data, _count) do
    case data do
      [s | rest] ->
        {{:bulk, s}, rest}
    end
  end

  def parse_simple(data) do
    case data do
      [s | rest] ->
        {{:simple, s}, rest}
    end
  end

  @doc """
  Parse the incoming redis command
  """
  def parse(data) do
    regex = ~r/^(\*|\$|\+)(\d+)$/
    case data do
      # check if it starts with *
      [s | rest] ->
        case Regex.run(regex, s) do
          [_, c, count] ->
            case c do
              "*" -> parse_array(rest, String.to_integer(count))
              "$" -> parse_bulk(rest, String.to_integer(count))
              "+" -> parse_simple(rest)
            end
          _ -> {:error, "Invalid request"}
        end
    end
  end

  def exec(data, client, p) do
    {parsed, _} = parse(String.split(data, "\r\n"))
    case parsed do
      # handle echo and ping commands
      {:array, [bulk: "echo", bulk: s]} ->
        :gen_tcp.send(client, encode_bulk(s))
      {:array, [bulk: "ping"]} ->
        :gen_tcp.send(client, "+PONG\r\n")
      {:array, [bulk: "get", bulk: key]} ->
        send(p, {:get, key, self()})
        receive do
          {:reply, value} ->
            :gen_tcp.send(client, encode_bulk(value))
        end
      {:array, [bulk: "set", bulk: key, bulk: value]} ->
        send(p, {:set, key, value, self()})
        receive do
          {:reply, :ok} ->
            :gen_tcp.send(client, "+OK\r\n")
        end
      {:array, [bulk: "delete", bulk: key]} ->
        send(p, {:delete, key, self()})
        receive do
          {:reply, value} ->
            :gen_tcp.send(client, encode_bulk(value))
        end
      _ -> :gen_tcp.send(client, "Invalid command\r\n")
    end
  end

  def encode_bulk(data) do
    "$#{byte_size(data)}\r\n#{data}\r\n"
  end

  def loop(client, p) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        exec(data, client, p)
        loop(client, p)
      {:error, :closed} ->
        IO.puts "Client disconnected"
    end
  end

  defp loop_acceptor(socket, p) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        spawn(fn -> handle_client(client, p) end)
        loop_acceptor(socket, p)
      {:error, reason} ->
        IO.puts "Error accepting connection: #{reason}"
    end
  end

  def handler() do
    receive do
      {:get, key, client} ->
        value = GenServer.call(KeyValue, {:get, key})
        case value do
          nil -> send(client, {:reply, "-1"})
          _ -> send(client, {:reply, value})
        end
      {:set, key, value, client} ->
        GenServer.call(KeyValue, {:set, key, value})
        send(client, {:reply, :ok})
      {:delete, key, client} ->
        value = GenServer.call(KeyValue, {:delete, key})
        case value do
          nil -> send(client, {:reply, "-1"})
          _ -> send(client, {:reply, value})
        end
    end
    handler()
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])

    KeyValue.start_link

    p = self()
    spawn(fn -> loop_acceptor(socket, p) end)
    handler()
  end
end
