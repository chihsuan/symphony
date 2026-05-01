defmodule SymphonyElixir.ObservabilityPubSubTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixirWeb.ObservabilityPubSub

  test "subscribe and broadcast_update deliver dashboard updates" do
    assert :ok = ObservabilityPubSub.subscribe()
    assert :ok = ObservabilityPubSub.broadcast_update()
    assert_receive :observability_updated
  end

  test "subscribe_transcript and broadcast_transcript_event deliver issue events" do
    event = %{event: :notification, payload: %{message: "live"}, timestamp: DateTime.utc_now()}

    assert :ok = ObservabilityPubSub.subscribe_transcript("issue-123")
    assert :ok = ObservabilityPubSub.broadcast_transcript_event("issue-123", event)
    assert_receive {:transcript_event, ^event}
    assert :ok = ObservabilityPubSub.broadcast_transcript_event("issue-123", :not_an_event)
  end

  test "broadcast_update is a no-op when pubsub is unavailable" do
    pubsub_child_id = Phoenix.PubSub.Supervisor

    on_exit(fn ->
      if Process.whereis(SymphonyElixir.PubSub) == nil do
        assert {:ok, _pid} =
                 Supervisor.restart_child(SymphonyElixir.Supervisor, pubsub_child_id)
      end
    end)

    assert is_pid(Process.whereis(SymphonyElixir.PubSub))
    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, pubsub_child_id)
    refute Process.whereis(SymphonyElixir.PubSub)

    assert :ok = ObservabilityPubSub.broadcast_update()
    assert :ok = ObservabilityPubSub.broadcast_transcript_event("issue-123", %{event: :notification})
  end
end
