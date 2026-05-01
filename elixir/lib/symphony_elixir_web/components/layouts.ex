defmodule SymphonyElixirWeb.Layouts do
  @moduledoc """
  Shared layouts for the observability dashboard.
  """

  use Phoenix.Component

  @spec root(map()) :: Phoenix.LiveView.Rendered.t()
  def root(assigns) do
    assigns = assign(assigns, :csrf_token, Plug.CSRFProtection.get_csrf_token())

    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="csrf-token" content={@csrf_token} />
        <title>Symphony Observability</title>
        <script defer src="/vendor/phoenix_html/phoenix_html.js"></script>
        <script defer src="/vendor/phoenix/phoenix.js"></script>
        <script defer src="/vendor/phoenix_live_view/phoenix_live_view.js"></script>
        <script>
          window.addEventListener("DOMContentLoaded", function () {
            var csrfToken = document
              .querySelector("meta[name='csrf-token']")
              ?.getAttribute("content");

            if (!window.Phoenix || !window.LiveView) return;

            var transcriptFilterHook = {
              mounted: function () {
                this.activeFilters = new Set();
                this.handleFilterClick = function (event) {
                  var button = event.target.closest("[data-transcript-filter]");
                  if (!button || !this.el.contains(button)) return;

                  event.preventDefault();

                  var filter = button.getAttribute("data-transcript-filter");
                  if (filter === "all") {
                    this.activeFilters.clear();
                  } else if (this.activeFilters.has(filter)) {
                    this.activeFilters.delete(filter);
                  } else {
                    this.activeFilters.add(filter);
                  }

                  this.applyFilters();
                }.bind(this);

                this.el.addEventListener("click", this.handleFilterClick);
                this.applyFilters();
              },
              updated: function () {
                this.applyFilters();
              },
              destroyed: function () {
                this.el.removeEventListener("click", this.handleFilterClick);
              },
              filterButtons: function () {
                return Array.from(this.el.querySelectorAll("[data-transcript-filter]"));
              },
              filterKinds: function () {
                return this.filterButtons()
                  .map(function (button) {
                    return button.getAttribute("data-transcript-filter");
                  })
                  .filter(function (filter) {
                    return filter && filter !== "all";
                  });
              },
              applyFilters: function () {
                var events = this.el.querySelector("[data-transcript-events]");
                if (!events) return;

                events.removeAttribute("data-filter-active");
                this.filterKinds().forEach(function (filter) {
                  events.removeAttribute("data-filter-" + filter);
                });

                if (this.activeFilters.size > 0) {
                  events.setAttribute("data-filter-active", "true");
                  this.activeFilters.forEach(function (filter) {
                    events.setAttribute("data-filter-" + filter, "true");
                  });
                }

                this.filterButtons().forEach(function (button) {
                  var filter = button.getAttribute("data-transcript-filter");
                  var pressed =
                    filter === "all" ? this.activeFilters.size === 0 : this.activeFilters.has(filter);

                  button.setAttribute("aria-pressed", pressed ? "true" : "false");
                }, this);
              }
            };

            var liveSocket = new window.LiveView.LiveSocket("/live", window.Phoenix.Socket, {
              params: {_csrf_token: csrfToken},
              hooks: {TranscriptFilter: transcriptFilterHook}
            });

            liveSocket.connect();
            window.liveSocket = liveSocket;
          });
        </script>
        <link rel="stylesheet" href="/dashboard.css" />
      </head>
      <body>
        {@inner_content}
      </body>
    </html>
    """
  end

  @spec app(map()) :: Phoenix.LiveView.Rendered.t()
  def app(assigns) do
    ~H"""
    <main class="app-shell">
      {@inner_content}
    </main>
    """
  end
end
