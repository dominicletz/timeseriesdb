defmodule Timeseriesdb.MixProject do
  use Mix.Project

  @url "https://github.com/dominicletz/timeseriesdb"
  def project do
    [
      app: :timeseriesdb,
      version: "1.0.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex
      description:
        "Log data to disk in zstd compressed files in strict monotonic order. Allows querying of data by time range.",
      package: [
        licenses: ["Apache-2.0"],
        maintainers: ["Dominic Letz"],
        links: %{"GitHub" => @url}
      ],
      # Docs
      name: "TimeSeriesDB",
      source_url: @url,
      docs: [
        # The main page in the docs
        main: "TimeSeriesDB",
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:benchee, "~> 1.0", only: :dev},
      {:dets_plus, "~> 1.0", only: :dev},
      {:ezstd, "~> 1.0"}
    ]
  end
end
