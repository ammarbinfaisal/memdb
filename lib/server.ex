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

  def loop(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, _data} ->
        :gen_tcp.send(client, "+PONG\r\n")
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
