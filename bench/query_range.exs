#!/usr/bin/env elixir

make_element = fn x ->
  y = :erlang.phash2(x)
  %{x: x, y: :erlang.crc32("#{y}"), z: String.duplicate("#{y}", 100)}
end

File.rm_rf!("testdata/bench_tdb")
File.rm_rf!("testdata/bench_dets")

{:ok, state} = TimeSeriesDB.start_link("testdata/bench_tdb")
{:ok, dets} = DetsPlus.open_file(:bench_dets, file: "testdata/bench_dets")

for x <- 1..100_000 do
  DetsPlus.insert(dets, {x, make_element.(x)})
  TimeSeriesDB.append_row(state, x, make_element.(x))
end

TimeSeriesDB.flush(state)
DetsPlus.sync(dets)

dets_range = fn dets, range ->
  Enum.flat_map(range, &DetsPlus.lookup(dets, &1))
end

ref = TimeSeriesDB.query_range(state, 90_000, 90_001)
^ref = dets_range.(dets, 90_000..90_001)

Benchee.run(%{
  "TimeSeriesDB.query_range" => fn -> TimeSeriesDB.query_range(state, 90_000, 95_000) end,
  "DetsPlus.lookup" => fn -> dets_range.(dets, 90_000..95_000) end
})
