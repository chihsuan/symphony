defmodule SymphonyElixir.URLUtilsTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.URLUtils

  test "present_url trims binary URLs and rejects empty or non-binary values" do
    assert URLUtils.present_url(" https://example.test/path ") == "https://example.test/path"
    assert URLUtils.present_url(" ") == nil
    assert URLUtils.present_url(nil) == nil
  end

  test "pull_request_url accepts primary and fallback metadata keys" do
    assert URLUtils.pull_request_url(%{pull_request_url: " https://github.com/example/repo/pull/1 "}) ==
             "https://github.com/example/repo/pull/1"

    assert URLUtils.pull_request_url(%{
             pull_request_url: "",
             pr_url: "https://github.com/example/repo/pull/2"
           }) == "https://github.com/example/repo/pull/2"

    assert URLUtils.pull_request_url(nil) == nil
  end
end
