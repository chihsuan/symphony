defmodule SymphonyElixir.GitHub.PullRequest do
  @moduledoc """
  Reads pull request lifecycle state through the GitHub CLI.
  """

  @type comment :: %{
          optional(:author) => String.t() | nil,
          optional(:body) => String.t() | nil,
          optional(:url) => String.t() | nil,
          optional(:kind) => String.t(),
          optional(:created_at) => DateTime.t() | nil,
          optional(:updated_at) => DateTime.t() | nil
        }

  @type activity :: %{
          pr_url: String.t(),
          state: String.t() | nil,
          review_decision: String.t() | nil,
          latest_activity_at: DateTime.t() | nil,
          latest_review_activity_at: DateTime.t() | nil,
          comments: [comment()]
        }

  @spec fetch_activity(term(), keyword()) :: {:ok, activity()} | {:error, term()}
  def fetch_activity(pr_url, opts \\ []) do
    if is_binary(pr_url) and is_list(opts) do
      do_fetch_activity(pr_url, opts)
    else
      {:error, :invalid_pr_url}
    end
  end

  defp do_fetch_activity(pr_url, opts) do
    with {:ok, pr} <- view_pr(pr_url, opts),
         {:ok, inline_comments} <- fetch_inline_comments(pr_url, pr, opts) do
      comments = pr_comments(pr) ++ review_comments(pr) ++ inline_comments
      latest_activity_at = latest_activity_at(pr, comments)
      latest_review_activity_at = latest_review_activity_at(comments)

      {:ok,
       %{
         pr_url: Map.get(pr, "url") || pr_url,
         state: Map.get(pr, "state"),
         review_decision: Map.get(pr, "reviewDecision"),
         latest_activity_at: latest_activity_at,
         latest_review_activity_at: latest_review_activity_at,
         comments: comments
       }}
    end
  end

  defp view_pr(pr_url, opts) do
    args = [
      "pr",
      "view",
      pr_url,
      "--json",
      "number,state,reviewDecision,updatedAt,comments,reviews,title,url"
    ]

    with {:ok, output} <- run_gh(args, opts),
         {:ok, pr} when is_map(pr) <- Jason.decode(output) do
      {:ok, pr}
    else
      {:ok, _decoded} -> {:error, :invalid_pr_payload}
      {:error, %Jason.DecodeError{} = error} -> {:error, {:invalid_pr_payload, Exception.message(error)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_inline_comments(pr_url, %{"number" => number}, opts) when is_integer(number) do
    case parse_github_pr_url(pr_url) do
      {:ok, host, owner, repo, _number} ->
        case run_gh(github_api_args(host, "repos/#{owner}/#{repo}/pulls/#{number}/comments"), opts) do
          {:ok, output} ->
            decode_inline_comments(output)

          {:error, {:gh_failed, _args, 404, _output}} ->
            {:ok, []}

          {:error, reason} ->
            {:error, reason}
        end

      :error ->
        {:ok, []}
    end
  end

  defp fetch_inline_comments(_pr_url, _pr, _opts), do: {:ok, []}

  defp decode_inline_comments(output) when is_binary(output) do
    case Jason.decode(output) do
      {:ok, comments} when is_list(comments) ->
        {:ok, Enum.map(comments, &normalize_inline_comment/1)}

      {:ok, _payload} ->
        {:ok, []}

      {:error, %Jason.DecodeError{} = error} ->
        {:error, {:invalid_inline_comments_payload, Exception.message(error)}}
    end
  end

  defp pr_comments(%{"comments" => comments}) when is_list(comments) do
    Enum.map(comments, fn comment ->
      %{
        kind: "comment",
        author: get_in(comment, ["author", "login"]),
        body: Map.get(comment, "body"),
        url: Map.get(comment, "url"),
        created_at: parse_datetime(Map.get(comment, "createdAt")),
        updated_at: parse_datetime(Map.get(comment, "updatedAt"))
      }
    end)
  end

  defp pr_comments(_pr), do: []

  defp review_comments(%{"reviews" => reviews}) when is_list(reviews) do
    reviews
    |> Enum.map(fn review ->
      %{
        kind: "review",
        author: get_in(review, ["author", "login"]),
        body: Map.get(review, "body"),
        url: Map.get(review, "url"),
        state: Map.get(review, "state"),
        created_at: parse_datetime(Map.get(review, "submittedAt")),
        updated_at: parse_datetime(Map.get(review, "submittedAt"))
      }
    end)
    |> Enum.reject(&(blank?(Map.get(&1, :body)) and blank?(Map.get(&1, :state))))
  end

  defp review_comments(_pr), do: []

  defp normalize_inline_comment(comment) when is_map(comment) do
    %{
      kind: "inline_comment",
      author: get_in(comment, ["user", "login"]),
      body: Map.get(comment, "body"),
      url: Map.get(comment, "html_url"),
      created_at: parse_datetime(Map.get(comment, "created_at")),
      updated_at: parse_datetime(Map.get(comment, "updated_at"))
    }
  end

  defp normalize_inline_comment(_comment), do: %{}

  defp latest_activity_at(pr, comments) do
    ([parse_datetime(Map.get(pr, "updatedAt"))] ++ Enum.flat_map(comments, &comment_timestamps/1))
    |> Enum.reject(&is_nil/1)
    |> Enum.max(DateTime, fn -> nil end)
  end

  defp latest_review_activity_at(comments) do
    comments
    |> Enum.flat_map(&comment_timestamps/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.max(DateTime, fn -> nil end)
  end

  defp comment_timestamps(comment) when is_map(comment) do
    [Map.get(comment, :updated_at), Map.get(comment, :created_at)]
  end

  defp comment_timestamps(_comment), do: []

  defp parse_github_pr_url(url) when is_binary(url) do
    case Regex.run(~r{^https://([^/\s]*github[^/\s]*)/([^/\s]+)/([^/\s]+)/pull/(\d+)(?:$|[/?#])}, url) do
      [_full, host, owner, repo, number] -> {:ok, host, owner, repo, String.to_integer(number)}
      _ -> :error
    end
  end

  defp github_api_args("github.com", endpoint), do: ["api", endpoint]
  defp github_api_args(host, endpoint), do: ["api", "--hostname", host, endpoint]

  defp run_gh(args, opts) when is_list(args) do
    cmd_opts = [stderr_to_stdout: true] ++ cwd_opt(Keyword.get(opts, :cwd))

    case gh_runner(opts).(args, cmd_opts) do
      {:ok, output} -> {:ok, output}
      {:error, reason} -> {:error, reason}
      {output, 0} -> {:ok, output}
      {output, status} -> {:error, {:gh_failed, args, status, output}}
    end
  rescue
    error in ErlangError -> {:error, {:gh_unavailable, Exception.message(error)}}
  end

  defp gh_runner(opts) do
    case Keyword.get(opts, :gh_runner) do
      runner when is_function(runner, 2) -> runner
      _ -> &System.cmd("gh", &1, &2)
    end
  end

  defp cwd_opt(cwd) when is_binary(cwd) and cwd != "", do: [cd: cwd]
  defp cwd_opt(_cwd), do: []

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> nil
    end
  end

  defp parse_datetime(_value), do: nil

  defp blank?(value) when is_binary(value), do: String.trim(value) == ""
  defp blank?(nil), do: true
  defp blank?(_value), do: false
end
