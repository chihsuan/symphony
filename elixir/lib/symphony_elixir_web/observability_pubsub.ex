defmodule SymphonyElixirWeb.ObservabilityPubSub do
  @moduledoc """
  PubSub helpers for observability dashboard updates.
  """

  @pubsub SymphonyElixir.PubSub
  @topic "observability:dashboard"
  @update_message :observability_updated
  @transcript_topic_prefix "transcript:"

  @spec subscribe() :: :ok | {:error, term()}
  def subscribe do
    Phoenix.PubSub.subscribe(@pubsub, @topic)
  end

  @spec broadcast_update() :: :ok
  def broadcast_update do
    case Process.whereis(@pubsub) do
      pid when is_pid(pid) ->
        Phoenix.PubSub.broadcast(@pubsub, @topic, @update_message)

      _ ->
        :ok
    end
  end

  @spec subscribe_transcript(String.t()) :: :ok | {:error, term()}
  def subscribe_transcript(issue_id) when is_binary(issue_id) do
    Phoenix.PubSub.subscribe(@pubsub, transcript_topic(issue_id))
  end

  @spec broadcast_transcript_event(String.t(), map()) :: :ok
  def broadcast_transcript_event(issue_id, event)
      when is_binary(issue_id) and is_map(event) do
    case Process.whereis(@pubsub) do
      pid when is_pid(pid) ->
        Phoenix.PubSub.broadcast(@pubsub, transcript_topic(issue_id), {:transcript_event, event})

      _ ->
        :ok
    end
  end

  def broadcast_transcript_event(_issue_id, _event), do: :ok

  @spec transcript_topic(String.t()) :: String.t()
  def transcript_topic(issue_id) when is_binary(issue_id) do
    @transcript_topic_prefix <> issue_id
  end
end
