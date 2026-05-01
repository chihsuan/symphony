defmodule SymphonyElixir.RunStoreTest do
  use SymphonyElixir.TestSupport

  setup do
    :ok = RunStore.clear()
  end

  test "persists run records, retry queue entries, and codex totals" do
    started_at = DateTime.utc_now()
    due_at = DateTime.add(started_at, 60_000, :millisecond)

    assert :ok =
             RunStore.put_run(%{
               run_id: "run-1",
               issue_id: "issue-1",
               issue_identifier: "RSM-1",
               title: "Persist me",
               state: "In Progress",
               status: "running",
               attempt: 2,
               started_at: started_at,
               ended_at: nil,
               error: nil,
               worker_host: "worker-a",
               workspace_path: "/tmp/workspaces/RSM-1",
               session_id: "thread-1-turn-1",
               transcript_path: "/tmp/transcript.jsonl",
               tokens: %{input_tokens: 10, output_tokens: 4, total_tokens: 14},
               runtime_seconds: 0
             })

    assert :ok =
             RunStore.update_run("run-1", %{
               status: "success",
               ended_at: DateTime.add(started_at, 10, :second),
               runtime_seconds: 10
             })

    assert [
             %{
               run_id: "run-1",
               issue_id: "issue-1",
               status: "success",
               attempt: 2,
               session_id: "thread-1-turn-1",
               transcript_path: "/tmp/transcript.jsonl",
               runtime_seconds: 10
             }
           ] = RunStore.list_runs()

    assert :ok =
             RunStore.put_retry(%{
               issue_id: "issue-1",
               issue_identifier: "RSM-1",
               identifier: "RSM-1",
               attempt: 3,
               due_at: due_at,
               error: "agent exited: :boom",
               worker_host: "worker-a",
               workspace_path: "/tmp/workspaces/RSM-1"
             })

    assert [
             %{
               issue_id: "issue-1",
               identifier: "RSM-1",
               attempt: 3,
               due_at: ^due_at,
               error: "agent exited: :boom"
             }
           ] = RunStore.list_retries()

    assert :ok = RunStore.delete_retry("issue-1")
    assert [] = RunStore.list_retries()

    assert :ok =
             RunStore.put_pr_lifecycle(%{
               issue_id: "issue-1",
               issue_identifier: "RSM-1",
               pr_url: "https://github.com/example/repo/pull/1",
               workspace_path: "/tmp/workspaces/RSM-1",
               status: "watching",
               updated_at: started_at
             })

    assert :ok =
             RunStore.update_pr_lifecycle("issue-1", %{
               status: "cooling_down",
               last_activity_at: due_at
             })

    assert [
             %{
               issue_id: "issue-1",
               pr_url: "https://github.com/example/repo/pull/1",
               workspace_path: "/tmp/workspaces/RSM-1",
               status: "cooling_down",
               last_activity_at: ^due_at
             }
           ] = RunStore.list_pr_lifecycles()

    assert :ok = RunStore.delete_pr_lifecycle("issue-1")
    assert [] = RunStore.list_pr_lifecycles()

    totals = %{input_tokens: 10, output_tokens: 4, total_tokens: 14, seconds_running: 10}
    assert :ok = RunStore.put_codex_totals(totals)
    assert totals == RunStore.get_codex_totals()
  end

  test "interrupt_running_runs marks stale running records as failures" do
    now = DateTime.utc_now()

    assert :ok =
             RunStore.put_run(%{
               run_id: "run-stale",
               issue_id: "issue-stale",
               issue_identifier: "RSM-2",
               status: "running",
               attempt: 1,
               started_at: now
             })

    assert {:ok, 1} = RunStore.interrupt_running_runs("orchestrator restarted before worker exit")

    assert [
             %{
               run_id: "run-stale",
               status: "failure",
               error: "orchestrator restarted before worker exit",
               ended_at: %DateTime{}
             }
           ] = RunStore.list_runs()
  end

  test "interrupt_running_runs skips malformed running records" do
    now = DateTime.utc_now()

    assert :ok =
             RunStore.put_run(%{
               run_id: "run-valid",
               issue_id: "issue-valid",
               issue_identifier: "RSM-3",
               status: "running",
               attempt: 1,
               started_at: now
             })

    assert {:atomic, :ok} =
             :mnesia.transaction(fn ->
               :mnesia.write({:symphony_run_store_runs, "malformed", %{status: "running"}})
               :ok
             end)

    log =
      capture_log(fn ->
        assert {:ok, 1} = RunStore.interrupt_running_runs("orchestrator restarted before worker exit")
      end)

    assert log =~ "Skipping malformed running run store record"

    assert [
             %{run_id: "run-valid", status: "failure"},
             %{status: "running"}
           ] = Enum.sort_by(RunStore.list_runs(:all), &Map.get(&1, :run_id, "zzz"))
  end
end
