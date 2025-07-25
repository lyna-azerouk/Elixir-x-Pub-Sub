defmodule BroadwayDemo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Goth, name: BroadwayDemo.Goth},
      BroadwayDemoWeb.Telemetry,
      BroadwayDemo.Repo,
      {DNSCluster, query: Application.get_env(:broadway_demo, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: BroadwayDemo.PubSub},
      # Start the Finch HTTP client for sending emails
      {Finch, name: BroadwayDemo.Finch},
      # Start a worker by calling: BroadwayDemo.Worker.start_link(arg)
      # {BroadwayDemo.Worker, arg},
      # Start to serve requests, typically the last entry
      BroadwayDemoWeb.Endpoint,
      {BroadwayDemo.BroadwayCustomProducer, []}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: BroadwayDemo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    BroadwayDemoWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
