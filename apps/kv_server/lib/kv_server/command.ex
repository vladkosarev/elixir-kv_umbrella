defmodule KVServer.Command do
  @doc ~S"""
  Parses the given `line` into a command.

  ## Examples

      iex> KVServer.Command.parse "CREATE shopping\r\n"
      {:ok, {:create, "shopping"}}

      iex> KVServer.Command.parse "CREATE  shopping  \r\n"
      {:ok, {:create, "shopping"}}

      iex> KVServer.Command.parse "PUT shopping milk 1\r\n"
      {:ok, {:put, "shopping", "milk", "1"}}

      iex> KVServer.Command.parse "GET shopping milk\r\n"
      {:ok, {:get, "shopping", "milk"}}

      iex> KVServer.Command.parse "DELETE shopping eggs\r\n"
      {:ok, {:delete, "shopping", "eggs"}}
      
      iex> KVServer.Command.parse "PUT shopping eggs 3\r\n"
      {:ok, {:put, "shopping", "eggs", "3"}}
      
      iex> KVServer.Command.parse "GET shopping\r\n"
      {:ok, {:get, "shopping"}}

  Unknown commands or commands with the wrong number of
  arguments return an error:

      iex> KVServer.Command.parse "UNKNOWN shopping eggs\r\n"
      {:error, :unknown_command}

      iex> KVServer.Command.parse "PUT shopping\r\n"
      {:error, :unknown_command}

  """
  
  def parse(line) do
    case String.split(line) do
      ["CREATE", bucket] -> {:ok, {:create, bucket}}      
      ["GET", bucket, key] -> {:ok, {:get, bucket, key}}
      ["GET", bucket] -> {:ok, {:get, bucket}}      
      ["PUT", bucket, key, value] -> {:ok, {:put, bucket, key, value}}
      ["DELETE", bucket, key] -> {:ok, {:delete, bucket, key}}
      _ -> {:error, :unknown_command}
    end
  end

  @doc """
  Runs the given command.
  """
  def run(command)

  def run({:create, bucket}) do
    KV.Registry.create(KV.Registry, bucket)
    {:ok, "OK\r\n"}
  end

  def run({:create, bucket}, pid) do
    KV.Registry.create(pid, bucket)
    {:ok, "OK\r\n"}
  end

  def run({:get, bucket, key}) do
    lookup bucket, fn pid ->
      value = KV.Bucket.get(pid, key)
      {:ok, "#{value}\r\nOK\r\n"}
    end
  end
  
  def run({:get, bucket}) do
    lookup bucket, fn pid ->
      value = KV.Bucket.get(pid) 
      {:ok, "#{bucket_to_string value}\r\nOK\r\n"}
    end
  end

  defp bucket_to_string(bucket) do
    bucket_to_string(bucket, "")
  end
    
  defp bucket_to_string(bucket,str) do
    case bucket do
      [] -> str
      [{key,value} | tail] -> bucket_to_string(tail, str <> key <> " - " <> value <> "\r\n")
    end      
  end

  def run({:put, bucket, key, value}) do
    lookup bucket, fn pid ->
      KV.Bucket.put(pid, key, value)
      {:ok, "OK\r\n"}
    end
  end

  def run({:delete, bucket, key}) do
    lookup bucket, fn pid ->
      KV.Bucket.delete(pid, key)
      {:ok, "OK\r\n"}
    end
  end

  defp lookup(bucket, callback) do
    case KV.Registry.lookup(KV.Registry, bucket) do
      {:ok, pid} -> callback.(pid)
      :error -> {:error, :not_found}
    end
  end
end