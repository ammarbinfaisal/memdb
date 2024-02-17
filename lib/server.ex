defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  def handle_client(client) do
    IO.puts "Client connected"
    loop(client)
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

  def split_and_parse(data) do
    {parsed, _} = parse(String.split(data, "\r\n"))
    parsed
  end

  def encode_bulk(data) do
    "$#{byte_size(data)}\r\n#{data}\r\n"
  end

  def reply(client, data) do
    # map over the parsed data and send the response
    case data do
      # handle echo and ping commands
      {:array, [bulk: "echo", bulk: s]} ->
        :gen_tcp.send(client, encode_bulk(s))
      {:array, [bulk: "ping"]} ->
        :gen_tcp.send(client, "+PONG\r\n")
      _ -> :gen_tcp.send(client, "Invalid command\r\n")
    end
  end

  def loop(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        parsed = split_and_parse(data)
        # IO.inspect parsed
        reply(client, parsed)
        loop(client)
      {:error, :closed} ->
        IO.puts "Client disconnected"
    end
  end

  defp loop_acceptor(socket) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        spawn(fn -> handle_client(client) end)
        loop_acceptor(socket)
      {:error, reason} ->
        IO.puts "Error accepting connection: #{reason}"
    end
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])

    # spawn a new process for each incoming connection
    loop_acceptor(socket)
  end
end
