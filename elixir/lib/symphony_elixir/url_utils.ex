defmodule SymphonyElixir.URLUtils do
  @moduledoc false

  @spec present_url(term()) :: String.t() | nil
  def present_url(url) when is_binary(url) do
    case String.trim(url) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  def present_url(_url), do: nil

  @spec pull_request_url(term()) :: String.t() | nil
  def pull_request_url(entry) when is_map(entry) do
    present_url(Map.get(entry, :pull_request_url)) || present_url(Map.get(entry, :pr_url))
  end

  def pull_request_url(_entry), do: nil
end
