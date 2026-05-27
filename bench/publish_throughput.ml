(** Throughput benchmark for [Producer.publish] / [Producer.publish_multi].

    Pre-creates a producer, publishes [-n] messages of [-size] bytes (optionally
    batched [-batch] per call), and reports wall time, CPU, throughput, and GC
    allocation stats. *)

open Eio.Std
open Nsq

let ok_or_failwith = function Ok v -> v | Error e -> failwith e

type args = {
  host : string;
  count : int;
  payload_size : int;
  batch_size : int;
  topic : string;
  pool_size : int;
}

let parse_args () =
  let host = ref "localhost" in
  let count = ref 100_000 in
  let payload_size = ref 200 in
  let batch_size = ref 1 in
  let topic = ref "bench" in
  let pool_size = ref 1 in
  let specs =
    [
      ("-host", Stdlib.Arg.Set_string host, " nsqd host (default: localhost)");
      ("-n", Stdlib.Arg.Set_int count, " total messages to publish");
      ("-size", Stdlib.Arg.Set_int payload_size, " payload bytes per message");
      ( "-batch",
        Stdlib.Arg.Set_int batch_size,
        " messages per call (1 = PUB, >1 = MPUB)" );
      ("-topic", Stdlib.Arg.Set_string topic, " topic name");
      ("-pool", Stdlib.Arg.Set_int pool_size, " producer pool size");
    ]
  in
  Stdlib.Arg.parse (Stdlib.Arg.align specs)
    (fun _ -> ())
    "publish_throughput [options]";
  {
    host = !host;
    count = !count;
    payload_size = !payload_size;
    batch_size = !batch_size;
    topic = !topic;
    pool_size = !pool_size;
  }

let pp_bytes f =
  if f >= 1024. ** 3. then Printf.sprintf "%.2f GiB" (f /. (1024. ** 3.))
  else if f >= 1024. ** 2. then Printf.sprintf "%.2f MiB" (f /. (1024. ** 2.))
  else if f >= 1024. then Printf.sprintf "%.2f KiB" (f /. 1024.)
  else Printf.sprintf "%.0f B" f

let publish_one ~p ~topic ~payload ~batch ~batch_size =
  let r =
    if batch_size = 1 then Producer.publish p topic payload
    else Producer.publish_multi p topic batch
  in
  match r with Ok () -> () | Error e -> failwith e

let run args =
  if args.batch_size < 1 then failwith "-batch must be >= 1";
  if args.count < args.batch_size then failwith "-n must be >= -batch";
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Switch.run @@ fun sw ->
  let p =
    Producer.create ~sw ~net ~clock ~pool_size:args.pool_size
      (Address.Host args.host)
    |> ok_or_failwith
  in
  let topic = Topic.Topic args.topic in
  let payload = Bytes.make args.payload_size 'x' in
  let batch = List.init args.batch_size (fun _ -> payload) in
  let batches = args.count / args.batch_size in
  let publish_one () =
    publish_one ~p ~topic ~payload ~batch ~batch_size:args.batch_size
  in

  (* Warm-up: one publish to populate the connection. Not counted. *)
  publish_one ();

  Stdlib.Gc.compact ();
  let gc0 = Stdlib.Gc.quick_stat () in
  let alloc0 = Stdlib.Gc.allocated_bytes () in
  let cpu0 = Unix.times () in
  let t0 = Eio.Time.now clock in
  for _ = 1 to batches do
    publish_one ()
  done;
  let t1 = Eio.Time.now clock in
  let cpu1 = Unix.times () in
  let alloc1 = Stdlib.Gc.allocated_bytes () in
  let gc1 = Stdlib.Gc.quick_stat () in

  let wall = t1 -. t0 in
  let user = cpu1.tms_utime -. cpu0.tms_utime in
  let sys = cpu1.tms_stime -. cpu0.tms_stime in
  let cpu_pct = if wall > 0. then (user +. sys) /. wall *. 100. else 0. in
  let total_msgs = batches * args.batch_size in
  let total_bytes = total_msgs * args.payload_size in
  let msgs_per_sec = Float.of_int total_msgs /. wall in
  let bytes_per_sec = Float.of_int total_bytes /. wall in
  let minor = gc1.minor_words -. gc0.minor_words in
  let promoted = gc1.promoted_words -. gc0.promoted_words in
  let major = gc1.major_words -. gc0.major_words in
  let allocated = alloc1 -. alloc0 in
  let per_msg f = f /. Float.of_int total_msgs in

  Stdlib.print_endline "publish_throughput";
  Stdlib.Printf.printf "  host:            %s\n" args.host;
  Stdlib.Printf.printf "  topic:           %s\n" args.topic;
  Stdlib.Printf.printf "  total messages:  %d\n" total_msgs;
  Stdlib.Printf.printf "  batch size:      %d (%d calls)\n" args.batch_size
    batches;
  Stdlib.Printf.printf "  payload size:    %d B\n" args.payload_size;
  Stdlib.Printf.printf "  pool size:       %d\n" args.pool_size;
  Stdlib.print_endline "";
  Stdlib.Printf.printf "  wall time:       %.3f s\n" wall;
  Stdlib.Printf.printf "  user CPU:        %.3f s\n" user;
  Stdlib.Printf.printf "  sys CPU:         %.3f s\n" sys;
  Stdlib.Printf.printf "  CPU usage:       %.1f %%\n" cpu_pct;
  Stdlib.Printf.printf "  throughput:      %.0f msgs/s   %s/s\n" msgs_per_sec
    (pp_bytes bytes_per_sec);
  Stdlib.print_endline "";
  Stdlib.Printf.printf "  allocated:       %s total, %.1f B/msg\n"
    (pp_bytes allocated) (per_msg allocated);
  Stdlib.Printf.printf "  minor words:     %.3e total, %.1f /msg\n" minor
    (per_msg minor);
  Stdlib.Printf.printf "  promoted words:  %.3e total, %.1f /msg\n" promoted
    (per_msg promoted);
  Stdlib.Printf.printf "  major words:     %.3e total, %.1f /msg\n" major
    (per_msg major);
  Stdlib.Printf.printf "  minor GCs:       %d\n"
    (gc1.minor_collections - gc0.minor_collections);
  Stdlib.Printf.printf "  major GCs:       %d\n"
    (gc1.major_collections - gc0.major_collections);
  Stdlib.Printf.printf "  compactions:     %d\n"
    (gc1.compactions - gc0.compactions)

let () = run (parse_args ())
