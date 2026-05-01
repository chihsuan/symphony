defmodule SymphonyElixir.GitHub.PullRequestTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.GitHub.PullRequest

  test "fetch_activity separates review timestamps from PR updates and supports enterprise hosts" do
    pr_url = "https://github.example.com/org/repo/pull/42"

    runner = fn
      ["pr", "view", ^pr_url, "--json", fields], opts ->
        assert fields == "number,state,reviewDecision,updatedAt,comments,reviews,title,url"
        assert opts[:stderr_to_stdout]

        {Jason.encode!(%{
           "number" => 42,
           "state" => "OPEN",
           "reviewDecision" => "APPROVED",
           "updatedAt" => "2026-05-01T10:00:00Z",
           "comments" => [],
           "reviews" => [
             %{
               "author" => %{"login" => "reviewer"},
               "body" => "Looks good.",
               "url" => "#{pr_url}#pullrequestreview-1",
               "state" => "APPROVED",
               "submittedAt" => "2026-05-01T09:00:00Z"
             }
           ],
           "url" => pr_url
         }), 0}

      ["api", "--hostname", "github.example.com", "repos/org/repo/pulls/42/comments"], opts ->
        assert opts[:stderr_to_stdout]

        {Jason.encode!([
           %{
             "user" => %{"login" => "reviewer"},
             "body" => "Nit fixed separately.",
             "html_url" => "#{pr_url}#discussion_r1",
             "created_at" => "2026-05-01T09:03:00Z",
             "updated_at" => "2026-05-01T09:05:00Z"
           }
         ]), 0}
    end

    assert {:ok, activity} = PullRequest.fetch_activity(pr_url, gh_runner: runner)

    assert activity.pr_url == pr_url
    assert activity.state == "OPEN"
    assert activity.review_decision == "APPROVED"
    assert activity.latest_activity_at == ~U[2026-05-01 10:00:00Z]
    assert activity.latest_review_activity_at == ~U[2026-05-01 09:05:00Z]
    assert Enum.map(activity.comments, & &1.kind) == ["review", "inline_comment"]
  end
end
