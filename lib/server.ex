defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    args = System.argv()
    port = args |> Enum.at(0)
    IO.puts("Starting server on port #{port}")
    port = case port do
      nil -> 6379
      _ -> String.to_integer(port)
    end

    IO.puts("Starting server on port #{port}")
    # if replicaof is set, then we are a slave
    # find --replicaof host port
    replicaof_host = args |> Enum.at(2)
    replicaof_port = args |> Enum.at(3)

    if replicaof_host && replicaof_port do
      IO.puts("Starting as a slave of #{replicaof_host}:#{replicaof_port} at port #{port}")
      listen(port, {replicaof_host, String.to_integer(replicaof_port)})
    else
      IO.puts("Starting as a master")
      listen(port, nil)
    end
  end

  def parse_array(data, count, acc \\ []) do
    case count do
      0 ->
        {{:array, acc}, data}

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

          _ ->
            {:error, "Invalid request"}
        end
    end
  end

  defp expire(key, ttl) do
    :timer.sleep(ttl)
    IO.puts("Expiring key #{key}")
    GenServer.call(KeyValue, {:delete, key})
  end

  def exec(data, client, info) do
    {parsed, _} = parse(String.split(data, "\r\n"))
    IO.inspect(parsed)

    case parsed do
      # handle echo and ping commands
      {:array, [bulk: "echo", bulk: s]} ->
        :gen_tcp.send(client, encode_bulk(s))

      {:array, [bulk: "ping"]} ->
        :gen_tcp.send(client, "+PONG\r\n")

      {:array, [bulk: "get", bulk: key]} ->
        value = GenServer.call(KeyValue, {:get, key})

        case value do
          nil -> :gen_tcp.send(client, "$-1\r\n")
          _ -> :gen_tcp.send(client, encode_bulk(value))
        end

      {:array, [bulk: "set", bulk: key, bulk: value]} ->
        GenServer.call(KeyValue, {:set, key, value})
        :gen_tcp.send(client, "+OK\r\n")

      {:array, [bulk: "set", bulk: key, bulk: value, bulk: "px", bulk: ttl]} ->
        GenServer.call(KeyValue, {:set, key, value})
        spawn(fn -> expire(key, String.to_integer(ttl)) end)
        :gen_tcp.send(client, "+OK\r\n")

      {:array, [bulk: "delete", bulk: key]} ->
        value = GenServer.call(KeyValue, {:delete, key})
        IO.inspect(value)
        :gen_tcp.send(client, "+OK\r\n")

      {:array, [bulk: "info", bulk: "replication"]} ->
        info =
          case info.role do
            "master" ->
              "role:master\nmaster_replid:#{info.run_id}\nmaster_repl_offset:0\n"

            "slave" ->
              "role:slave\nmaster_host:#{info.master_host}\nmaster_port:#{info.master_port}\n"
          end

        :gen_tcp.send(client, encode_bulk(info))

      {:array, [bulk: "replconf", bulk: "listening-port", bulk: port]} ->
        IO.puts("Listening port set to #{port}")
        :gen_tcp.send(client, "+OK\r\n")

      {:array, [bulk: "replconf", bulk: "capa", bulk: "psync2"]} ->
        IO.puts("PSYNC2 enabled")
        :gen_tcp.send(client, "+OK\r\n")

      {:array, [bulk: "psync", bulk: "?", bulk: "-1"]} ->
        IO.puts("PSYNC requested")
        :gen_tcp.send(client, "+OK\r\n")

      _ ->
        :gen_tcp.send(client, "Invalid command\r\n")
    end
  end

  def encode_bulk(data) do
    "$#{byte_size(data)}\r\n#{data}\r\n"
  end

  def encode_array(data, length) do
    "*#{length}\r\n#{Enum.join(data)}"
  end

  def loop(client, master) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        exec(data, client, master)
        loop(client, master)

      {:error, :closed} ->
        IO.puts("Client disconnected")
    end
  end

  defp loop_acceptor(socket, info) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        spawn(fn -> loop(client, info) end)
        loop_acceptor(socket, info)

      {:error, reason} ->
        IO.puts("Error accepting connection: #{reason}")
    end
  end

  @doc """
  Listen for incoming connections
  """
  def listen(port, master) do
    res = :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true])

    case res do
      {:ok, socket} ->
        KeyValue.start_link()
        run_id = Base.encode16(:crypto.strong_rand_bytes(20))

        info =
          case master do
            nil ->
              %{
                role: "master",
                run_id: run_id
              }

            {master_host, master_port} ->
              opts = [:binary, :inet, active: false, packet: :line]

              ip =
                case master_host do
                  "localhost" ->
                    {127, 0, 0, 1}

                  _ ->
                    String.split(master_host, ".")
                    |> Enum.map(&String.to_integer/1)
                    |> List.to_tuple()
                end

              {:ok, conn} = :gen_tcp.connect(ip, port, opts)

              :gen_tcp.send(conn, ["ping"] |> Enum.map(&encode_bulk/1) |> Enum.join())
              IO.inspect(:gen_tcp.recv(conn, 0))

              :gen_tcp.send(
                conn,
                ["replconf", "listening-port", Integer.to_string(port)]
                |> Enum.map(&encode_bulk/1)
                |> Enum.join()
              )

              IO.inspect(:gen_tcp.recv(conn, 0))

              :gen_tcp.send(
                conn,
                ["replconf", "capa", "psync2"] |> Enum.map(&encode_bulk/1) |> Enum.join()
              )

              # :gen_tcp.send(conn, encode_array([encode_bulk("psync"), encode_bulk("?"), encode_bulk("-1")], 3))
              :gen_tcp.send(conn, ["psync", "?", "-1"] |> Enum.map(&encode_bulk/1) |> Enum.join())
              IO.inspect(:gen_tcp.recv(conn, 0))
              :gen_tcp.close(conn)

              %{
                role: "slave",
                run_id: run_id,
                master_host: master_host,
                master_port: master_port
              }
          end

        loop_acceptor(socket, info)

      {:error, err} ->
        IO.inspect(err)
        System.halt(1)
    end
  end
end
