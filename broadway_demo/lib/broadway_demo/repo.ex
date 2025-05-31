defmodule BroadwayDemo.Repo do
  use Ecto.Repo,
    otp_app: :broadway_demo,
    adapter: Ecto.Adapters.Postgres
end
