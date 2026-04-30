defmodule SymphonyElixir.Workspace do
  @moduledoc """
  Creates isolated per-issue workspaces for parallel Codex agents.
  """

  require Logger
  alias SymphonyElixir.{Config, PathSafety, SSH}

  @remote_workspace_marker "__SYMPHONY_WORKSPACE__"

  @type worker_host :: String.t() | nil

  @spec create_for_issue(map() | String.t() | nil, worker_host()) ::
          {:ok, Path.t()} | {:error, term()}
  def create_for_issue(issue_or_identifier, worker_host \\ nil) do
    issue_context = issue_context(issue_or_identifier)

    try do
      safe_id = safe_identifier(issue_context.issue_identifier)

      with {:ok, workspace} <- workspace_path_for_issue(safe_id, worker_host),
           :ok <- validate_workspace_path(workspace, worker_host),
           {:ok, workspace, created?} <- ensure_workspace(workspace, issue_context, worker_host),
           :ok <- maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
        {:ok, workspace}
      end
    rescue
      error in [ArgumentError, ErlangError, File.Error] ->
        Logger.error("Workspace creation failed #{issue_log_context(issue_context)} worker_host=#{worker_host_for_log(worker_host)} error=#{Exception.message(error)}")
        {:error, error}
    end
  end

  defp ensure_workspace(workspace, issue_context, worker_host) do
    case Config.settings!().workspace.strategy do
      "worktree" ->
        ensure_worktree_workspace(workspace, issue_context, worker_host)

      _strategy ->
        ensure_directory_workspace(workspace, worker_host)
    end
  end

  defp ensure_directory_workspace(workspace, nil) do
    cond do
      File.dir?(workspace) ->
        {:ok, workspace, false}

      File.exists?(workspace) ->
        File.rm_rf!(workspace)
        create_workspace(workspace)

      true ->
        create_workspace(workspace)
    end
  end

  defp ensure_directory_workspace(workspace, worker_host) when is_binary(worker_host) do
    script =
      [
        "set -eu",
        remote_shell_assign("workspace", workspace),
        "if [ -d \"$workspace\" ]; then",
        "  created=0",
        "elif [ -e \"$workspace\" ]; then",
        "  rm -rf \"$workspace\"",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "else",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "fi",
        "cd \"$workspace\"",
        "printf '%s\\t%s\\t%s\\n' '#{@remote_workspace_marker}' \"$created\" \"$(pwd -P)\""
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {output, 0}} ->
        parse_remote_workspace_output(output)

      {:ok, {output, status}} ->
        {:error, {:workspace_prepare_failed, worker_host, status, output}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_worktree_workspace(workspace, issue_context, nil) do
    with {:ok, repo} <- local_worktree_repo(),
         :ok <- maybe_fetch_worktree_repo(repo),
         {:ok, created?} <- add_or_reuse_local_worktree(repo, workspace, worktree_branch(issue_context)) do
      {:ok, workspace, created?}
    end
  end

  defp ensure_worktree_workspace(workspace, issue_context, worker_host) when is_binary(worker_host) do
    settings = Config.settings!()
    branch = worktree_branch(issue_context)

    script =
      [
        "set -eu",
        remote_shell_assign("repo", settings.workspace.repo || ""),
        remote_shell_assign("workspace", workspace),
        "branch=#{shell_escape(branch)}",
        "if [ -z \"$repo\" ]; then",
        "  echo \"workspace_repo_missing: workspace.repo is required for worktree strategy\"",
        "  exit 41",
        "fi",
        "if [ ! -d \"$repo\" ]; then",
        "  echo \"workspace_repo_missing: $repo\"",
        "  exit 41",
        "fi",
        "git -C \"$repo\" rev-parse --git-dir >/dev/null",
        settings.workspace.fetch_before_dispatch && "git -C \"$repo\" fetch origin",
        "if [ -d \"$workspace\" ]; then",
        "  registered=$(git -C \"$repo\" worktree list --porcelain | awk '/^worktree / {print substr($0, 10)}' | grep -Fx \"$workspace\" || true)",
        "  if [ -z \"$registered\" ]; then",
        "    echo \"workspace_not_registered_worktree: $workspace\"",
        "    exit 42",
        "  fi",
        "  created=0",
        "elif [ -e \"$workspace\" ]; then",
        "  rm -rf \"$workspace\"",
        "  #{remote_worktree_add_command()}",
        "  created=1",
        "else",
        "  #{remote_worktree_add_command()}",
        "  created=1",
        "fi",
        "cd \"$workspace\"",
        "printf '%s\\t%s\\t%s\\n' '#{@remote_workspace_marker}' \"$created\" \"$(pwd -P)\""
      ]
      |> Enum.reject(&(&1 in ["", nil, false]))
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, settings.hooks.timeout_ms) do
      {:ok, {output, 0}} ->
        parse_remote_workspace_output(output)

      {:ok, {output, status}} ->
        {:error, {:workspace_prepare_failed, worker_host, status, output}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_workspace(workspace) do
    File.rm_rf!(workspace)
    File.mkdir_p!(workspace)
    {:ok, workspace, true}
  end

  defp local_worktree_repo do
    repo = Config.settings!().workspace.repo

    if is_binary(repo) and String.trim(repo) != "" do
      {:ok, Path.expand(repo)}
    else
      {:error, :missing_workspace_repo}
    end
  end

  defp maybe_fetch_worktree_repo(repo) do
    case Config.settings!().workspace.fetch_before_dispatch do
      true -> run_git(repo, ["fetch", "origin"])
      false -> :ok
    end
  end

  defp add_or_reuse_local_worktree(repo, workspace, branch) do
    cond do
      File.dir?(workspace) ->
        case registered_worktree?(repo, workspace) do
          true -> {:ok, false}
          false -> {:error, {:workspace_not_registered_worktree, workspace}}
        end

      File.exists?(workspace) ->
        File.rm_rf!(workspace)
        add_local_worktree(repo, workspace, branch)

      true ->
        add_local_worktree(repo, workspace, branch)
    end
  end

  defp add_local_worktree(repo, workspace, branch) do
    File.mkdir_p!(Path.dirname(workspace))

    with :ok <- run_git(repo, worktree_add_args(repo, workspace, branch)) do
      {:ok, true}
    end
  end

  defp worktree_add_args(repo, workspace, branch) do
    case git_branch_exists?(repo, branch) do
      true -> ["worktree", "add", workspace, branch]
      false -> ["worktree", "add", "-b", branch, workspace, "HEAD"]
    end
  end

  defp remote_worktree_add_command do
    "if git -C \"$repo\" rev-parse --verify \"refs/heads/$branch\" >/dev/null 2>&1; then git -C \"$repo\" worktree add \"$workspace\" \"$branch\"; else git -C \"$repo\" worktree add -b \"$branch\" \"$workspace\" HEAD; fi"
  end

  @spec remove(Path.t()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace), do: remove(workspace, nil)

  @spec remove(Path.t(), worker_host()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace, nil) do
    issue_context = workspace_issue_context(workspace)

    remove_workspace(workspace, issue_context, nil)
  end

  def remove(workspace, worker_host) when is_binary(worker_host) do
    issue_context = workspace_issue_context(workspace)

    remove_workspace(workspace, issue_context, worker_host)
  end

  defp remove_workspace(workspace, issue_context, nil) do
    if Config.settings!().workspace.strategy == "worktree" do
      remove_worktree_workspace(workspace, issue_context, nil)
    else
      remove_directory_workspace(workspace, issue_context, nil)
    end
  end

  defp remove_workspace(workspace, issue_context, worker_host) when is_binary(worker_host) do
    if Config.settings!().workspace.strategy == "worktree" do
      remove_worktree_workspace(workspace, issue_context, worker_host)
    else
      remove_directory_workspace(workspace, issue_context, worker_host)
    end
  end

  defp remove_directory_workspace(workspace, issue_context, nil) do
    case File.exists?(workspace) do
      true ->
        case validate_workspace_path(workspace, nil) do
          :ok ->
            maybe_run_before_remove_hook(workspace, issue_context, nil)
            File.rm_rf(workspace)

          {:error, reason} ->
            {:error, reason, ""}
        end

      false ->
        File.rm_rf(workspace)
    end
  end

  defp remove_directory_workspace(workspace, issue_context, worker_host) when is_binary(worker_host) do
    maybe_run_before_remove_hook(workspace, issue_context, worker_host)

    script =
      [
        remote_shell_assign("workspace", workspace),
        "rm -rf \"$workspace\""
      ]
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {_output, 0}} ->
        {:ok, []}

      {:ok, {output, status}} ->
        {:error, {:workspace_remove_failed, worker_host, status, output}, ""}

      {:error, reason} ->
        {:error, reason, ""}
    end
  end

  defp remove_worktree_workspace(workspace, issue_context, nil) do
    with {:ok, repo} <- local_worktree_repo(),
         :ok <- validate_workspace_path(workspace, nil),
         :ok <- remove_local_worktree(repo, workspace, issue_context) do
      {:ok, [workspace]}
    else
      {:error, reason, output} -> {:error, reason, output}
      {:error, reason} -> {:error, reason, ""}
    end
  end

  defp remove_worktree_workspace(workspace, issue_context, worker_host) when is_binary(worker_host) do
    settings = Config.settings!()
    branch = worktree_branch(issue_context)

    maybe_run_before_remove_hook(workspace, issue_context, worker_host)

    script =
      [
        "set -eu",
        remote_shell_assign("repo", settings.workspace.repo || ""),
        remote_shell_assign("workspace", workspace),
        "branch=#{shell_escape(branch)}",
        "if [ -z \"$repo\" ]; then",
        "  echo \"workspace_repo_missing: workspace.repo is required for worktree strategy\"",
        "  exit 41",
        "fi",
        "if [ ! -d \"$repo\" ]; then",
        "  echo \"workspace_repo_missing: $repo\"",
        "  exit 41",
        "fi",
        "git -C \"$repo\" rev-parse --git-dir >/dev/null",
        "registered=$(git -C \"$repo\" worktree list --porcelain | awk '/^worktree / {print substr($0, 10)}' | grep -Fx \"$workspace\" || true)",
        "if [ -n \"$registered\" ]; then",
        "  git -C \"$repo\" worktree remove --force \"$workspace\"",
        "elif [ -e \"$workspace\" ]; then",
        "  echo \"workspace_not_registered_worktree: $workspace\"",
        "  exit 42",
        "fi",
        "if git -C \"$repo\" rev-parse --verify \"refs/heads/$branch\" >/dev/null 2>&1; then",
        "  git -C \"$repo\" branch -D \"$branch\"",
        "fi"
      ]
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, settings.hooks.timeout_ms) do
      {:ok, {_output, 0}} ->
        {:ok, []}

      {:ok, {output, status}} ->
        {:error, {:workspace_remove_failed, worker_host, status, output}, ""}

      {:error, reason} ->
        {:error, reason, ""}
    end
  end

  @spec remove_issue_workspaces(term()) :: :ok
  def remove_issue_workspaces(identifier), do: remove_issue_workspaces(identifier, nil)

  @spec remove_issue_workspaces(term(), worker_host()) :: :ok
  def remove_issue_workspaces(%{identifier: identifier} = issue, worker_host)
      when is_binary(identifier) and is_binary(worker_host) do
    remove_issue_workspace(identifier, issue_context(issue), worker_host)
  end

  def remove_issue_workspaces(%{identifier: identifier} = issue, nil) when is_binary(identifier) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        remove_issue_workspace(identifier, issue_context(issue), nil)

      worker_hosts ->
        Enum.each(worker_hosts, &remove_issue_workspaces(issue, &1))
    end

    :ok
  end

  def remove_issue_workspaces(identifier, worker_host) when is_binary(identifier) and is_binary(worker_host) do
    remove_issue_workspace(identifier, issue_context(identifier), worker_host)
  end

  def remove_issue_workspaces(identifier, nil) when is_binary(identifier) do
    issue_context = issue_context(identifier)

    case Config.settings!().worker.ssh_hosts do
      [] ->
        remove_issue_workspace(identifier, issue_context, nil)

      worker_hosts ->
        Enum.each(worker_hosts, &remove_issue_workspace(identifier, issue_context, &1))
    end

    :ok
  end

  def remove_issue_workspaces(_identifier, _worker_host) do
    :ok
  end

  defp remove_issue_workspace(identifier, issue_context, worker_host) when is_binary(identifier) do
    safe_id = safe_identifier(identifier)

    case workspace_path_for_issue(safe_id, worker_host) do
      {:ok, workspace} ->
        case remove_workspace(workspace, issue_context, worker_host) do
          {:ok, _removed_paths} ->
            :ok

          {:error, reason, output} ->
            log_workspace_removal_failure(workspace, issue_context, worker_host, reason, output)
        end

        :ok

      {:error, reason} ->
        Logger.warning("Workspace removal skipped #{issue_log_context(issue_context)} identifier=#{identifier} worker_host=#{worker_host_for_log(worker_host)} reason=#{inspect(reason)}")
        :ok
    end
  end

  @spec run_before_run_hook(Path.t(), map() | String.t() | nil, worker_host()) ::
          :ok | {:error, term()}
  def run_before_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.hooks_for_issue(issue_context)

    case hooks.before_run do
      nil ->
        :ok

      command ->
        run_hook(
          command,
          workspace,
          issue_context,
          "before_run",
          worker_host,
          hooks.timeout_ms
        )
    end
  end

  @spec run_after_run_hook(Path.t(), map() | String.t() | nil, worker_host()) :: :ok
  def run_after_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.hooks_for_issue(issue_context)

    case hooks.after_run do
      nil ->
        :ok

      command ->
        run_hook(
          command,
          workspace,
          issue_context,
          "after_run",
          worker_host,
          hooks.timeout_ms
        )
        |> ignore_hook_failure()
    end
  end

  defp workspace_path_for_issue(safe_id, nil) when is_binary(safe_id) do
    Config.settings!().workspace.root
    |> Path.join(safe_id)
    |> PathSafety.canonicalize()
  end

  defp workspace_path_for_issue(safe_id, worker_host) when is_binary(safe_id) and is_binary(worker_host) do
    {:ok, Path.join(Config.settings!().workspace.root, safe_id)}
  end

  defp safe_identifier(identifier) do
    String.replace(identifier || "issue", ~r/[^a-zA-Z0-9._-]/, "_")
  end

  defp worktree_branch(%{issue_identifier: identifier}) when is_binary(identifier) and identifier != "" do
    "auto/#{identifier}"
  end

  defp worktree_branch(_issue_context), do: "auto/issue"

  defp maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
    hooks = Config.hooks_for_issue(issue_context)

    case created? do
      true ->
        case hooks.after_create do
          nil ->
            :ok

          command ->
            run_hook(
              command,
              workspace,
              issue_context,
              "after_create",
              worker_host,
              hooks.timeout_ms
            )
        end

      false ->
        :ok
    end
  end

  defp maybe_run_before_remove_hook(workspace, issue_context, nil) do
    hooks = Config.hooks_for_issue(issue_context)

    case File.dir?(workspace) do
      true ->
        case hooks.before_remove do
          nil ->
            :ok

          command ->
            run_hook(
              command,
              workspace,
              issue_context,
              "before_remove",
              nil,
              hooks.timeout_ms
            )
            |> ignore_hook_failure()
        end

      false ->
        :ok
    end
  end

  defp maybe_run_before_remove_hook(workspace, issue_context, worker_host) when is_binary(worker_host) do
    hooks = Config.hooks_for_issue(issue_context)

    case hooks.before_remove do
      nil ->
        :ok

      command ->
        script =
          [
            remote_shell_assign("workspace", workspace),
            "if [ -d \"$workspace\" ]; then",
            "  cd \"$workspace\"",
            "  #{command}",
            "fi"
          ]
          |> Enum.join("\n")

        run_remote_command(worker_host, script, hooks.timeout_ms)
        |> case do
          {:ok, {output, status}} ->
            handle_hook_command_result(
              {output, status},
              workspace,
              issue_context,
              "before_remove"
            )

          {:error, {:workspace_hook_timeout, "before_remove", _timeout_ms} = reason} ->
            {:error, reason}

          {:error, reason} ->
            {:error, reason}
        end
        |> ignore_hook_failure()
    end
  end

  defp ignore_hook_failure(:ok), do: :ok
  defp ignore_hook_failure({:error, _reason}), do: :ok

  defp run_hook(command, workspace, issue_context, hook_name, nil, timeout_ms) do
    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local")

    task =
      Task.async(fn ->
        System.cmd("sh", ["-lc", command], cd: workspace, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      nil ->
        Task.shutdown(task, :brutal_kill)

        Logger.warning("Workspace hook timed out hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local timeout_ms=#{timeout_ms}")

        {:error, {:workspace_hook_timeout, hook_name, timeout_ms}}
    end
  end

  defp run_hook(command, workspace, issue_context, hook_name, worker_host, timeout_ms) when is_binary(worker_host) do
    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=#{worker_host}")

    case run_remote_command(worker_host, "cd #{shell_escape(workspace)} && #{command}", timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      {:error, {:workspace_hook_timeout, ^hook_name, _timeout_ms} = reason} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_hook_command_result({_output, 0}, _workspace, _issue_id, _hook_name) do
    :ok
  end

  defp handle_hook_command_result({output, status}, workspace, issue_context, hook_name) do
    sanitized_output = sanitize_hook_output_for_log(output)

    Logger.warning("Workspace hook failed hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} status=#{status} output=#{inspect(sanitized_output)}")

    {:error, {:workspace_hook_failed, hook_name, status, output}}
  end

  defp sanitize_hook_output_for_log(output, max_bytes \\ 2_048) do
    binary_output = IO.iodata_to_binary(output)

    case byte_size(binary_output) <= max_bytes do
      true ->
        binary_output

      false ->
        binary_part(binary_output, 0, max_bytes) <> "... (truncated)"
    end
  end

  defp log_workspace_removal_failure(workspace, issue_context, worker_host, reason, output) do
    sanitized_output = sanitize_hook_output_for_log(output)

    Logger.warning(
      "Workspace removal failed #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=#{worker_host_for_log(worker_host)} reason=#{inspect(reason)} output=#{inspect(sanitized_output)}"
    )
  end

  defp validate_workspace_path(workspace, nil) when is_binary(workspace) do
    expanded_workspace = Path.expand(workspace)
    expanded_root = Path.expand(Config.settings!().workspace.root)
    expanded_root_prefix = expanded_root <> "/"

    with {:ok, canonical_workspace} <- PathSafety.canonicalize(expanded_workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(expanded_root) do
      canonical_root_prefix = canonical_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:workspace_equals_root, canonical_workspace, canonical_root}}

        String.starts_with?(canonical_workspace <> "/", canonical_root_prefix) ->
          :ok

        String.starts_with?(expanded_workspace <> "/", expanded_root_prefix) ->
          {:error, {:workspace_symlink_escape, expanded_workspace, canonical_root}}

        true ->
          {:error, {:workspace_outside_root, canonical_workspace, canonical_root}}
      end
    else
      {:error, {:path_canonicalize_failed, path, reason}} ->
        {:error, {:workspace_path_unreadable, path, reason}}
    end
  end

  defp validate_workspace_path(workspace, worker_host)
       when is_binary(workspace) and is_binary(worker_host) do
    cond do
      String.trim(workspace) == "" ->
        {:error, {:workspace_path_unreadable, workspace, :empty}}

      String.contains?(workspace, ["\n", "\r", <<0>>]) ->
        {:error, {:workspace_path_unreadable, workspace, :invalid_characters}}

      true ->
        :ok
    end
  end

  defp remote_shell_assign(variable_name, raw_path)
       when is_binary(variable_name) and is_binary(raw_path) do
    [
      "#{variable_name}=#{shell_escape(raw_path)}",
      "case \"$#{variable_name}\" in",
      "  '~') #{variable_name}=\"$HOME\" ;;",
      "  '~/'*) " <> variable_name <> "=\"$HOME/${" <> variable_name <> "#~/}\" ;;",
      "esac"
    ]
    |> Enum.join("\n")
  end

  defp parse_remote_workspace_output(output) do
    lines = String.split(IO.iodata_to_binary(output), "\n", trim: true)

    payload =
      Enum.find_value(lines, fn line ->
        case String.split(line, "\t", parts: 3) do
          [@remote_workspace_marker, created, path] when created in ["0", "1"] and path != "" ->
            {created == "1", path}

          _ ->
            nil
        end
      end)

    case payload do
      {created?, workspace} when is_boolean(created?) and is_binary(workspace) ->
        {:ok, workspace, created?}

      _ ->
        {:error, {:workspace_prepare_failed, :invalid_output, output}}
    end
  end

  defp remove_local_worktree(repo, workspace, issue_context) do
    cond do
      registered_worktree?(repo, workspace) ->
        maybe_run_before_remove_hook(workspace, issue_context, nil)

        with :ok <- run_git(repo, ["worktree", "remove", "--force", workspace]) do
          delete_local_worktree_branch(repo, worktree_branch(issue_context))
        end

      File.exists?(workspace) ->
        {:error, {:workspace_not_registered_worktree, workspace}, ""}

      true ->
        delete_local_worktree_branch(repo, worktree_branch(issue_context))
    end
  end

  defp delete_local_worktree_branch(repo, branch) do
    case git_branch_exists?(repo, branch) do
      true -> run_git(repo, ["branch", "-D", branch])
      false -> :ok
    end
  end

  defp registered_worktree?(repo, workspace) do
    workspace = Path.expand(workspace)

    case git_output(repo, ["worktree", "list", "--porcelain"]) do
      {:ok, output} ->
        output
        |> String.split("\n", trim: true)
        |> Enum.filter(&String.starts_with?(&1, "worktree "))
        |> Enum.map(&String.replace_prefix(&1, "worktree ", ""))
        |> Enum.any?(&(Path.expand(&1) == workspace))

      {:error, _reason, _output} ->
        false
    end
  end

  defp git_branch_exists?(repo, branch) do
    case System.cmd("git", ["-C", repo, "rev-parse", "--verify", "refs/heads/#{branch}"], stderr_to_stdout: true) do
      {_output, 0} -> true
      {_output, _status} -> false
    end
  end

  defp run_git(repo, args) when is_binary(repo) and is_list(args) do
    case git_output(repo, args) do
      {:ok, _output} -> :ok
      {:error, reason, output} -> {:error, reason, output}
    end
  end

  defp git_output(repo, args) when is_binary(repo) and is_list(args) do
    case System.cmd("git", ["-C", repo | args], stderr_to_stdout: true) do
      {output, 0} ->
        {:ok, output}

      {output, status} ->
        {:error, {:git_failed, repo, args, status}, output}
    end
  end

  defp run_remote_command(worker_host, script, timeout_ms)
       when is_binary(worker_host) and is_binary(script) and is_integer(timeout_ms) and timeout_ms > 0 do
    task =
      Task.async(fn ->
        SSH.run(worker_host, script, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:error, {:workspace_hook_timeout, "remote_command", timeout_ms}}
    end
  end

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp workspace_issue_context(workspace) do
    %{
      issue_id: nil,
      issue_identifier: Path.basename(workspace),
      labels: []
    }
  end

  defp issue_context(%{id: issue_id, identifier: identifier} = issue) do
    %{
      issue_id: issue_id,
      issue_identifier: identifier || "issue",
      labels: issue_labels(issue)
    }
  end

  defp issue_context(identifier) when is_binary(identifier) do
    %{
      issue_id: nil,
      issue_identifier: identifier,
      labels: []
    }
  end

  defp issue_context(_identifier) do
    %{
      issue_id: nil,
      issue_identifier: "issue",
      labels: []
    }
  end

  defp issue_labels(%{labels: labels}) when is_list(labels), do: labels
  defp issue_labels(%{"labels" => labels}) when is_list(labels), do: labels
  defp issue_labels(_issue), do: []

  defp issue_log_context(%{issue_id: issue_id, issue_identifier: issue_identifier}) do
    "issue_id=#{issue_id || "n/a"} issue_identifier=#{issue_identifier || "issue"}"
  end
end
