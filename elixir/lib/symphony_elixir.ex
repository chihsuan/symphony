defmodule SymphonyElixir do
  @moduledoc """
  Entry point for the Symphony orchestrator.
  """

  @doc """
  Start the orchestrator in the current BEAM node.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    SymphonyElixir.Orchestrator.start_link(opts)
  end
end

defmodule SymphonyElixir.Application do
  @moduledoc """
  OTP application entrypoint that starts core supervisors and workers.
  """

  use Application

  @impl true
  def start(_type, _args) do
    :ok = SymphonyElixir.LogFile.configure()

    children = child_specs_for_runtime()

    Supervisor.start_link(
      children,
      strategy: :one_for_one,
      name: SymphonyElixir.Supervisor
    )
  end

  @impl true
  def stop(_state) do
    SymphonyElixir.StatusDashboard.render_offline_status()
    :ok
  end

  @doc false
  @spec child_specs_for_runtime(map()) :: [Supervisor.child_spec() | module() | {module(), term()}]
  def child_specs_for_runtime(env \\ System.get_env()) when is_map(env) do
    core_children = [
      {Phoenix.PubSub, name: SymphonyElixir.PubSub},
      {Task.Supervisor, name: SymphonyElixir.TaskSupervisor},
      SymphonyElixir.WorkflowStore
    ]

    if orchestrator_runtime_disabled?(env) do
      core_children
    else
      core_children ++
        [
          SymphonyElixir.RunStore,
          SymphonyElixir.Orchestrator,
          SymphonyElixir.HttpServer,
          SymphonyElixir.StatusDashboard
        ]
    end
  end

  defp orchestrator_runtime_disabled?(env) do
    truthy_env?(Map.get(env, "SYMPHONY_DISABLE_ORCHESTRATOR")) ||
      (truthy_env?(Map.get(env, "SYMPHONY_AGENT_RUNTIME")) && Map.get(env, "MIX_ENV") != "test")
  end

  defp truthy_env?(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> then(&(&1 in ["1", "true", "yes", "on"]))
  end

  defp truthy_env?(_value), do: false
end
