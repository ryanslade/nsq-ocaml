open Base
open Lwt
open Lwt.Syntax

let client_version = "0.5.3"

let default_nsqd_port = 4150

let default_lookupd_port = 4161

let network_buffer_size = 16 * 1024

let lookupd_error_threshold = 5

module Seconds = struct
  type t = float [@@deriving yojson]

  let of_float f = f

  let value s = s
end

let recalculate_rdy_interval = Seconds.of_float 60.0

let default_backoff = Seconds.of_float 5.0

let max_backoff = Seconds.of_float 600.0

module Milliseconds = struct
  type t = int64 [@@deriving yojson]

  let of_int64 i = i

  let value i = i

  let of_seconds s = Seconds.value s *. 1000.0 |> Int64.of_float |> of_int64
end

let default_requeue_delay = Milliseconds.of_int64 5000L

module MessageID = struct
  type t = MessageID of bytes

  let of_bytes b = MessageID b

  let to_string = function MessageID id -> Bytes.to_string id
end

let ephemeral s = s ^ "#ephemeral"

module Topic = struct
  type t = Topic of string | TopicEphemeral of string

  let to_string = function Topic s -> s | TopicEphemeral s -> ephemeral s

  let%expect_test "to_string" =
    List.iter
      ~f:(fun h -> to_string h |> Stdio.print_endline)
      [ Topic "topic"; TopicEphemeral "topic" ];
    [%expect {|
    topic
    topic#ephemeral |}]
end

module Channel = struct
  type t = Channel of string | ChannelEphemeral of string

  let to_string = function Channel s -> s | ChannelEphemeral s -> ephemeral s

  let%expect_test "to_string" =
    List.iter
      ~f:(fun h -> to_string h |> Stdio.print_endline)
      [ Channel "chan"; ChannelEphemeral "chan" ];
    [%expect {|
    chan
    chan#ephemeral |}]
end

module FrameType = struct
  type t = FrameResponse | FrameError | FrameMessage | FrameUnknown of int32

  let of_int32 = function
    | 0l -> FrameResponse
    | 1l -> FrameError
    | 2l -> FrameMessage
    | i -> FrameUnknown i
end

type raw_frame = { frame_type : int32; data : Bytes.t }

type raw_message = {
  timestamp : int64;
  attempts : Unsigned.UInt16.t;
  id : MessageID.t;
  body : Bytes.t;
}

module Address = struct
  type t = Host of string | HostPort of string * int
  [@@deriving sexp_of, compare, hash, equal]

  let host s = Host s

  let host_port a p = HostPort (a, p)

  let to_string a =
    match a with Host a -> a | HostPort (a, p) -> Printf.sprintf "%s:%d" a p

  let%expect_test "to_string" =
    List.iter
      ~f:(fun h -> to_string h |> Stdio.print_endline)
      [ Host "example.com"; HostPort ("example.com", 123) ];
    [%expect {|
      example.com
      example.com:123 |}]
end

module IdentifyConfig = struct
  (* This will be encoded to JSON and sent with the IDENTIFY command *)
  type t = {
    heartbeat_interval : Milliseconds.t;
    client_id : string;
    hostname : string;
    user_agent : string;
    output_buffer_size : int;
    (* buffer size on bytes the server should use  *)
    output_buffer_timeout : Milliseconds.t;
    sample_rate : int;
  }
  [@@deriving yojson { strict = false }]
end

type command =
  | IDENTIFY of IdentifyConfig.t
  | MAGIC
  | SUB of Topic.t * Channel.t
  | PUB of Topic.t * Bytes.t
  | MPUB of Topic.t * Bytes.t list
  | REQ of MessageID.t * Milliseconds.t
  | FIN of MessageID.t
  | RDY of int
  | NOP

let try_with_string f =
  match Result.try_with f with
  | Ok _ as x -> x
  | Error e -> Error (Exn.to_string e)

module ServerMessage = struct
  type t =
    | ResponseOk
    | Heartbeat
    | ErrorInvalid of string
    | ErrorBadTopic of string
    | ErrorBadChannel of string
    | ErrorFINFailed of string
    | ErrorREQFailed of string
    | ErrorTOUCHFailed of string
    | Message of raw_message

  let to_string = function
    | ResponseOk -> "OK"
    | Heartbeat -> "Heartbeat"
    | ErrorInvalid s -> Printf.sprintf "ErrorInvalid: %s" s
    | ErrorBadTopic s -> Printf.sprintf "ErrorBadTopic: %s" s
    | ErrorBadChannel s -> Printf.sprintf "ErrorBadChannel: %s" s
    | ErrorFINFailed s -> Printf.sprintf "ErrorFINFailed: %s" s
    | ErrorREQFailed s -> Printf.sprintf "ErrorREQFailed: %s" s
    | ErrorTOUCHFailed s -> Printf.sprintf "ErrTouchFailed: %s" s
    | Message m ->
        Printf.sprintf "Message with ID %s" (MessageID.to_string m.id)

  let parse_response_body body =
    let body = Bytes.to_string body in
    match body with
    | "OK" -> Ok ResponseOk
    | "_heartbeat_" -> Ok Heartbeat
    | _ -> Error (Printf.sprintf "Unknown response: %s" body)

  let parse_message_body_exn body =
    let open Stdlib in
    let timestamp = Bytes.get_int64_be body 0 in
    let attempts = Bytes.get_uint16_be body 8 |> Unsigned.UInt16.of_int in
    let length = Bytes.length body in
    let id = MessageID.of_bytes (Bytes.sub body 10 16) in
    let body = Bytes.sub body 26 (length - 26) in
    Message { timestamp; attempts; id; body }

  let parse_message_body body =
    try_with_string (fun () -> parse_message_body_exn body)

  let parse_error_body body =
    let open Result in
    match String.split ~on:' ' (Bytes.to_string body) with
    | [ code; detail ] -> (
        match code with
        | "E_INVALID" -> return (ErrorInvalid detail)
        | "E_BAD_TOPIC" -> return (ErrorBadTopic detail)
        | "E_BAD_CHANNEL" -> return (ErrorBadChannel detail)
        | "E_FIN_FAILED" -> return (ErrorFINFailed detail)
        | "E_REQ_FAILED" -> return (ErrorREQFailed detail)
        | "E_TOUCH_FAILED" -> return (ErrorTOUCHFailed detail)
        | _ -> fail (Printf.sprintf "Unknown error code: %s. %s" code detail) )
    | _ ->
        fail (Printf.sprintf "Malformed error code: %s" (Bytes.to_string body))

  let of_raw_frame raw =
    match FrameType.of_int32 raw.frame_type with
    | FrameResponse -> parse_response_body raw.data
    | FrameMessage -> parse_message_body raw.data
    | FrameError -> parse_error_body raw.data
    | FrameUnknown i ->
        Result.Error (Printf.sprintf "Unknown frame type: %li" i)
end

module Lookup = struct
  type producer = {
    remote_address : string;
    hostname : string;
    broadcast_address : string;
    tcp_port : int;
    http_port : int;
    version : string;
  }
  [@@deriving yojson { strict = false }]

  type response = { channels : string list; producers : producer list }
  [@@deriving yojson { strict = false }]

  let response_of_string s =
    let open Result in
    try_with_string (fun () -> Yojson.Safe.from_string s) >>= response_of_yojson

  let producer_addresses lr =
    List.map
      ~f:(fun p -> Address.host_port p.broadcast_address p.tcp_port)
      lr.producers
end

let log_and_return prefix r =
  match r with
  | Ok _ as ok -> return ok
  | Error s as error ->
      let* () = Logs_lwt.err (fun l -> l "%s: %s" prefix s) in
      return error

let query_nsqlookupd ~topic a =
  let host, port =
    match a with
    | Address.Host h -> (h, default_lookupd_port)
    | Address.HostPort (h, p) -> (h, p)
  in
  let topic_string = Topic.to_string topic in
  let uri =
    Uri.make ~scheme:"http" ~host ~port ~path:"lookup"
      ~query:[ ("topic", [ topic_string ]) ]
      ()
  in
  let open Cohttp in
  catch
    (fun () ->
      let* resp, body = Cohttp_lwt_unix.Client.get uri in
      match Response.status resp with
      | `OK ->
          let* body = Cohttp_lwt.Body.to_string body in
          log_and_return "Error parsing lookup response"
            (Lookup.response_of_string body)
      | status ->
          return_error
            (Printf.sprintf "Expected %s, got %s"
               (Code.string_of_status `OK)
               (Code.string_of_status status)))
    (fun e ->
      let s = Exn.to_string e in
      log_and_return "Querying lookupd" (Error s))

(* Buffer shared between functions that produce packets *)
let shared_buffer = Buffer.create 1000000

(* 
    PUB <topic_name>\n
    [ 4-byte size in bytes ][ N-byte binary data ]
    
    <topic_name> - a valid string (optionally having #ephemeral suffix) 
*)
let bytes_of_pub =
  (* Preallocate buffers that we can reuse between runs *)
  let len_buf = Bytes.create 4 in
  let prefix = "PUB " in
  fun topic data ->
    Stdlib.Bytes.set_int32_be len_buf 0 (Int32.of_int_exn (Bytes.length data));
    Buffer.reset shared_buffer;
    Buffer.add_string shared_buffer prefix;
    Buffer.add_string shared_buffer (Topic.to_string topic);
    Buffer.add_string shared_buffer "\n";
    Buffer.add_bytes shared_buffer len_buf;
    Buffer.add_bytes shared_buffer data;
    Buffer.contents_bytes shared_buffer

let%expect_test "bytes_of_pub" =
  let topic = Topic.Topic "TestTopic" in
  let data = Bytes.of_string "Hello World" in
  bytes_of_pub topic data |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  (* Run it twice to ensure our shared buffers don't break anything *)
  let topic = Topic.Topic "AnotherTopic" in
  let data = Bytes.of_string "Another message" in
  bytes_of_pub topic data |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  [%expect
    {|
        00000000: 5055 4220 5465 7374 546f 7069 630a 0000
        00000001: 000b 4865 6c6c 6f20 576f 726c 64
        00000000: 5055 4220 416e 6f74 6865 7254 6f70 6963
        00000001: 0a00 0000 0f41 6e6f 7468 6572 206d 6573
        00000002: 7361 6765
      |}]

(* 
   MPUB <topic_name>\n
   [ 4-byte body size ]
   [ 4-byte num messages ]
   [ 4-byte message #1 size ][ N-byte binary data ]
      ... (repeated <num_messages> times)

   <topic_name> - a valid string (optionally having #ephemeral suffix)
*)
let bytes_of_mpub =
  let header = Bytes.create 8 in
  let size_buf = Bytes.create 4 in
  let prefix = "MPUB " in
  fun topic bodies ->
    let body_size =
      List.fold_left ~f:(fun a b -> a + Bytes.length b) ~init:0 bodies
    in
    let num_messages = List.length bodies in
    Buffer.reset shared_buffer;
    Buffer.add_string shared_buffer prefix;
    Buffer.add_string shared_buffer (Topic.to_string topic);
    Buffer.add_string shared_buffer "\n";
    Stdlib.Bytes.set_int32_be header 0 (Int32.of_int_exn body_size);
    Stdlib.Bytes.set_int32_be header 4 (Int32.of_int_exn num_messages);
    Buffer.add_bytes shared_buffer header;
    List.iter
      ~f:(fun data ->
        Stdlib.Bytes.set_int32_be size_buf 0
          (Int32.of_int_exn (Bytes.length data));
        Buffer.add_bytes shared_buffer size_buf;
        Buffer.add_bytes shared_buffer data)
      bodies;
    Buffer.contents_bytes shared_buffer

let%expect_test "bytes_of_mpub" =
  let topic = Topic.Topic "TestTopic" in
  let bodies =
    [ Bytes.of_string "Hello world"; Bytes.of_string "Hello again" ]
  in
  bytes_of_mpub topic bodies |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  [%expect
    {|
        00000000: 4d50 5542 2054 6573 7454 6f70 6963 0a00
        00000001: 0000 1600 0000 0200 0000 0b48 656c 6c6f
        00000002: 2077 6f72 6c64 0000 000b 4865 6c6c 6f20
        00000003: 6167 6169 6e
      |}]

(*
   IDENTIFY\n
   [ 4-byte size in bytes ][ N-byte JSON data ]
*)
let bytes_of_identify c =
  let prefix = "IDENTIFY\n" in
  let buf = Bytes.create 4 in
  let data = IdentifyConfig.to_yojson c |> Yojson.Safe.to_string in
  let length = String.length data in
  Stdlib.Bytes.set_int32_be buf 0 (Int32.of_int_exn length);
  Buffer.reset shared_buffer;
  Buffer.add_string shared_buffer prefix;
  Buffer.add_bytes shared_buffer buf;
  Buffer.add_string shared_buffer data;
  Buffer.contents_bytes shared_buffer

let%expect_test "bytes_of_identify" =
  let open IdentifyConfig in
  let id =
    {
      heartbeat_interval = 10L;
      client_id = "test";
      hostname = "test";
      user_agent = "test";
      output_buffer_size = 100;
      output_buffer_timeout = 60L;
      sample_rate = 50;
    }
  in
  bytes_of_identify id |> Hex.of_bytes |> Hex.hexdump ~print_chars:false;
  [%expect
    {|
        00000000: 4944 454e 5449 4659 0a00 0000 977b 2268
        00000001: 6561 7274 6265 6174 5f69 6e74 6572 7661
        00000002: 6c22 3a31 302c 2263 6c69 656e 745f 6964
        00000003: 223a 2274 6573 7422 2c22 686f 7374 6e61
        00000004: 6d65 223a 2274 6573 7422 2c22 7573 6572
        00000005: 5f61 6765 6e74 223a 2274 6573 7422 2c22
        00000006: 6f75 7470 7574 5f62 7566 6665 725f 7369
        00000007: 7a65 223a 3130 302c 226f 7574 7075 745f
        00000008: 6275 6666 6572 5f74 696d 656f 7574 223a
        00000009: 3630 2c22 7361 6d70 6c65 5f72 6174 6522
        00000010: 3a35 307d
      |}]

let bytes_of_command = function
  | MAGIC -> Bytes.of_string "  V2"
  | IDENTIFY c -> bytes_of_identify c
  | NOP -> Bytes.of_string "NOP\n"
  | RDY i -> Printf.sprintf "RDY %i\n" i |> Bytes.of_string
  | FIN id ->
      Printf.sprintf "FIN %s\n" (MessageID.to_string id) |> Bytes.of_string
  | SUB (t, c) ->
      Printf.sprintf "SUB %s %s\n" (Topic.to_string t) (Channel.to_string c)
      |> Bytes.of_string
  | REQ (id, delay) ->
      Printf.sprintf "REQ %s %Li\n" (MessageID.to_string id)
        (Milliseconds.value delay)
      |> Bytes.of_string
  | PUB (t, data) -> bytes_of_pub t data
  | MPUB (t, data) -> bytes_of_mpub t data

let%expect_test "bytes_of_command" =
  let commands =
    [
      MAGIC;
      IDENTIFY
        {
          heartbeat_interval = Milliseconds.of_int64 5000L;
          client_id = "client";
          hostname = "hostname";
          user_agent = "user_agent";
          output_buffer_size = 1024;
          output_buffer_timeout = Milliseconds.of_int64 1000L;
          sample_rate = 50;
        };
      NOP;
      RDY 10;
      SUB (Topic "Test", Channel "Test");
      REQ (MessageID.of_bytes (Bytes.of_string "ABC"), 100L);
      FIN (MessageID.of_bytes (Bytes.of_string "ABC"));
    ]
  in
  List.iter
    ~f:(fun c ->
      bytes_of_command c |> Hex.of_bytes |> Hex.hexdump ~print_chars:false)
    commands;
  [%expect
    {|
        00000000: 2020 5632
        00000000: 4944 454e 5449 4659 0a00 0000 a87b 2268
        00000001: 6561 7274 6265 6174 5f69 6e74 6572 7661
        00000002: 6c22 3a35 3030 302c 2263 6c69 656e 745f
        00000003: 6964 223a 2263 6c69 656e 7422 2c22 686f
        00000004: 7374 6e61 6d65 223a 2268 6f73 746e 616d
        00000005: 6522 2c22 7573 6572 5f61 6765 6e74 223a
        00000006: 2275 7365 725f 6167 656e 7422 2c22 6f75
        00000007: 7470 7574 5f62 7566 6665 725f 7369 7a65
        00000008: 223a 3130 3234 2c22 6f75 7470 7574 5f62
        00000009: 7566 6665 725f 7469 6d65 6f75 7422 3a31
        00000010: 3030 302c 2273 616d 706c 655f 7261 7465
        00000011: 223a 3530 7d
        00000000: 4e4f 500a
        00000000: 5244 5920 3130 0a
        00000000: 5355 4220 5465 7374 2054 6573 740a
        00000000: 5245 5120 4142 4320 3130 300a
        00000000: 4649 4e20 4142 430a
      |}]

(* Timeout will only apply if > 0.0 *)
let maybe_timeout ~timeout f =
  let timeout = Seconds.value timeout in
  if Float.(timeout <= 0.0) then f () else Lwt_unix.with_timeout timeout f

let send ~timeout ~conn command =
  let oc = snd conn in
  let data = bytes_of_command command in
  maybe_timeout ~timeout (fun () ->
      Lwt_io.write_from_exactly oc data 0 (Bytes.length data))

let catch_result promise =
  try_bind promise
    (fun x -> return_ok x)
    (fun e -> return_error (Exn.to_string e))

let catch_result1 f x = catch_result (fun () -> f x)

let connect host timeout =
  let host, port =
    match host with
    | Address.Host h -> (h, default_nsqd_port)
    | Address.HostPort (h, p) -> (h, p)
  in
  let* info = Lwt_unix.gethostbyname host in
  let host = Array.random_element_exn info.h_addr_list in
  let addr = Unix.ADDR_INET (host, port) in
  maybe_timeout ~timeout (fun () ->
      Lwt_io.open_connection
        ~in_buffer:(Lwt_bytes.create network_buffer_size)
        ~out_buffer:(Lwt_bytes.create network_buffer_size)
        addr)

let frame_from_bytes bytes =
  let frame_type = Stdlib.Bytes.get_int32_be bytes 0 in
  let to_read = Bytes.length bytes - 4 in
  let data = Bytes.sub ~pos:4 ~len:to_read bytes in
  { frame_type; data }

let read_raw_frame ~timeout (ic, _) =
  let* size = Lwt_io.BE.read_int32 ic in
  let size = Int32.to_int_exn size in
  let bytes = Bytes.create size in
  let* () =
    maybe_timeout ~timeout (fun () -> Lwt_io.read_into_exactly ic bytes 0 size)
  in
  return (frame_from_bytes bytes)

let send_expect_ok ~read_timeout ~write_timeout ~conn cmd =
  let* () = send ~timeout:write_timeout ~conn cmd in
  let* raw = read_raw_frame ~timeout:read_timeout conn in
  match ServerMessage.of_raw_frame raw with
  | Ok ResponseOk -> return_unit
  | Ok sm ->
      failwith
        (Printf.sprintf "Expected OK, got %s" (ServerMessage.to_string sm))
  | Error e -> failwith (Printf.sprintf "Expected OK, got %s" e)

let subscribe ~read_timeout ~write_timeout ~conn topic channel =
  send_expect_ok ~read_timeout ~write_timeout ~conn (SUB (topic, channel))

let identify ~read_timeout ~write_timeout ~conn ic =
  send_expect_ok ~read_timeout ~write_timeout ~conn (IDENTIFY ic)

module Consumer = struct
  module Config = struct
    type t = {
      (* The total number of messages allowed in flight for all connections of this consumer *)
      max_in_flight : int;
      max_attempts : int;
      backoff_multiplier : float;
      error_threshold : int;
      (* After how many errors do we send RDY 0 and back off *)

      (* network timeouts in seconds *)
      dial_timeout : Seconds.t;
      read_timeout : Seconds.t;
      write_timeout : Seconds.t;
      lookupd_poll_interval : Seconds.t;
      lookupd_poll_jitter : float;
      max_requeue_delay : Seconds.t;
      default_requeue_delay : Seconds.t;
      (* The fields below are used in IdentifyConfig.t *)
      heartbeat_interval : Seconds.t;
      client_id : string;
      hostname : string;
      user_agent : string;
      output_buffer_size : int;
      (* buffer size on bytes the server should use  *)
      output_buffer_timeout : Seconds.t;
      sample_rate : int; (* Between 0 and 99 *)
    }
    [@@deriving fields]

    let validate t =
      let module V = Validate in
      let w check = V.field_folder t check in
      let bound_int ~min ~max =
        Int.validate_bound ~min:(Maybe_bound.Incl min)
          ~max:(Maybe_bound.Incl max)
      in
      let bound_float ~min ~max =
        Float.validate_bound ~min:(Maybe_bound.Incl min)
          ~max:(Maybe_bound.Incl max)
      in
      let string_not_blank s = String.is_empty s |> not in
      V.of_list
        (Fields.fold ~init:[] ~max_in_flight:(w Int.validate_positive)
           ~max_attempts:(w (bound_int ~min:0 ~max:65535))
           ~dial_timeout:(w (bound_float ~min:0.1 ~max:300.0))
           ~read_timeout:(w (bound_float ~min:0.1 ~max:300.0))
           ~write_timeout:(w (bound_float ~min:0.1 ~max:300.0))
           ~lookupd_poll_interval:(w (bound_float ~min:0.1 ~max:300.0))
           ~default_requeue_delay:(w (bound_float ~min:0.0 ~max:3600.0))
           ~max_requeue_delay:(w (bound_float ~min:0.0 ~max:3600.0))
           ~lookupd_poll_jitter:(w (bound_float ~min:0.0 ~max:1.0))
           ~output_buffer_timeout:(w (bound_float ~min:0.01 ~max:300.0))
           ~output_buffer_size:(w (bound_int ~min:64 ~max:(5 * 1025 * 1000)))
           ~sample_rate:(w (bound_int ~min:0 ~max:99))
           ~heartbeat_interval:(w (bound_float ~min:1.0 ~max:300.0))
           ~client_id:(w (V.booltest string_not_blank ~if_false:"blank"))
           ~hostname:(w (V.booltest string_not_blank ~if_false:"blank"))
           ~user_agent:(w (V.booltest string_not_blank ~if_false:"blank"))
           ~error_threshold:(w (bound_int ~min:1 ~max:10000))
           ~backoff_multiplier:(w Float.validate_positive))

    let create ?(max_in_flight = 1) ?(max_attempts = 5)
        ?(backoff_multiplier = 0.5) ?(error_threshold = 1)
        ?(dial_timeout = Seconds.of_float 1.0)
        ?(read_timeout = Seconds.of_float 60.0)
        ?(write_timeout = Seconds.of_float 1.0)
        ?(lookupd_poll_interval = Seconds.of_float 60.0)
        ?(lookupd_poll_jitter = 0.3)
        ?(heartbeat_interval = Seconds.of_float 60.0)
        ?(max_requeue_delay = Seconds.of_float (15.0 *. 60.0))
        ?(default_requeue_delay = Seconds.of_float 90.0)
        ?(client_id = Unix.gethostname ()) ?(hostname = Unix.gethostname ())
        ?(user_agent = Printf.sprintf "nsq-ocaml/%s" client_version)
        ?(output_buffer_size = 16 * 1024)
        ?(output_buffer_timeout = Seconds.of_float 0.25) ?(sample_rate = 0) () =
      let t =
        {
          max_in_flight;
          max_attempts;
          backoff_multiplier;
          error_threshold;
          dial_timeout;
          read_timeout;
          write_timeout;
          lookupd_poll_interval;
          lookupd_poll_jitter;
          max_requeue_delay;
          default_requeue_delay;
          heartbeat_interval;
          client_id;
          hostname;
          user_agent;
          output_buffer_size;
          output_buffer_timeout;
          sample_rate;
        }
      in
      Validate.valid_or_error t validate
      |> Result.map_error ~f:Error.to_string_hum

    let to_identity_config c =
      {
        IdentifyConfig.heartbeat_interval =
          Milliseconds.of_seconds c.heartbeat_interval;
        IdentifyConfig.client_id = c.client_id;
        IdentifyConfig.hostname = c.hostname;
        IdentifyConfig.user_agent = c.user_agent;
        IdentifyConfig.output_buffer_size = c.output_buffer_size;
        IdentifyConfig.output_buffer_timeout =
          Milliseconds.of_seconds c.output_buffer_timeout;
        IdentifyConfig.sample_rate = c.sample_rate;
      }
  end

  type t = {
    addresses : Address.t list;
    (* The number of open NSQD connections *)
    open_connections : Address.t Hash_set.t;
    topic : Topic.t;
    channel : Channel.t;
    handler : bytes -> [ `Ok | `Requeue ] Lwt.t;
    config : Config.t;
    mode : [ `Nsqd | `Lookupd ];
    log_prefix : string;
  }

  type breaker_position = Closed | HalfOpen | Open

  type breaker_state = { position : breaker_position; error_count : int }

  let backoff_duration ~multiplier ~error_count =
    let bo = multiplier *. Float.of_int error_count in
    Float.min bo max_backoff |> Seconds.of_float

  let%expect_test "backoff_duration" =
    let test_cases = [ (1.0, 1); (1.0, 2); (2.0, 4000) ] in
    List.iteri
      ~f:(fun i (multiplier, error_count) ->
        let r = backoff_duration ~multiplier ~error_count in
        Stdio.print_endline
          (Printf.sprintf "Case %d: Backoff = %f" i (Seconds.value r)))
      test_cases;
    [%expect
      {|
      Case 0: Backoff = 1.000000
      Case 1: Backoff = 2.000000
      Case 2: Backoff = 600.000000 |}]

  let create ?(mode = `Nsqd)
      ?(config = Config.create () |> Result.ok_or_failwith) addresses topic
      channel handler =
    let open_connections = Hash_set.create (module Address) in
    {
      addresses;
      open_connections;
      topic;
      channel;
      handler;
      config;
      mode;
      log_prefix =
        Printf.sprintf "%s/%s" (Topic.to_string topic)
          (Channel.to_string channel);
    }

  let rdy_per_connection c =
    let connection_count = Hash_set.length c.open_connections in
    if connection_count = 0 || c.config.max_in_flight < connection_count then 1
    else c.config.max_in_flight / connection_count

  let do_after_async ~duration f =
    async (fun () ->
        let* () =
          Logs_lwt.debug (fun l ->
              l "Sleeping for %f seconds" (Seconds.value duration))
        in
        let* () = Lwt_unix.sleep (Seconds.value duration) in
        f ())

  let handle_message handler msg max_attempts =
    let requeue_delay attempts =
      let d = Milliseconds.value default_requeue_delay in
      let attempts = Int64.of_int_exn attempts in
      Milliseconds.of_int64 Int64.(d * attempts)
    in
    let* handler_result =
      let* res = catch_result1 handler msg.body in
      match res with
      | Ok r -> return r
      | Error s ->
          let* () = Logs_lwt.err (fun l -> l "Handler error: %s" s) in
          return `Requeue
    in
    match handler_result with
    | `Ok -> return (FIN msg.id)
    | `Requeue ->
        let attempts = Unsigned.UInt16.to_int msg.attempts in
        if attempts >= max_attempts then
          let* () =
            Logs_lwt.warn (fun l ->
                l "Discarding message %s as reached max attempts, %d"
                  (MessageID.to_string msg.id)
                  attempts)
          in
          return (FIN msg.id)
        else
          let delay = requeue_delay attempts in
          return (REQ (msg.id, delay))

  let handle_server_message server_message handler max_attempts =
    let warn_return_none name msg =
      let* () = Logs_lwt.warn (fun l -> l "%s: %s" name msg) in
      return_none
    in
    let open ServerMessage in
    match server_message with
    | ResponseOk -> return_none
    | Heartbeat ->
        let* () = Logs_lwt.debug (fun l -> l "Received heartbeat") in
        return_some NOP
    | ErrorInvalid s -> warn_return_none "ErrorInvalid" s
    | ErrorBadTopic s -> warn_return_none "ErrorBadTopic" s
    | ErrorBadChannel s -> warn_return_none "ErrorBadChannel" s
    | ErrorFINFailed s -> warn_return_none "ErrorFINFailed" s
    | ErrorREQFailed s -> warn_return_none "ErrorREQFailed" s
    | ErrorTOUCHFailed s -> warn_return_none "ErrorTOUCHFailed" s
    | Message msg ->
        let* cmd = handle_message handler msg max_attempts in
        return_some cmd

  type loop_message =
    | RawFrame of raw_frame
    | Command of command
    | TrialBreaker
    | ConnectionError of string
    | RecalcRDY

  let read_loop ~timeout conn mbox =
    let put_async m = async (fun () -> Lwt_mvar.put mbox m) in
    let rec loop () =
      let* res = catch_result1 (read_raw_frame ~timeout) conn in
      match res with
      | Ok raw ->
          put_async (RawFrame raw);
          loop ()
      | Error s ->
          put_async (ConnectionError s);
          return_unit
    in
    loop ()

  let maybe_open_breaker c conn mbox bs =
    let bs = { bs with error_count = bs.error_count + 1 } in
    let* () =
      Logs_lwt.warn (fun l ->
          l "Handler returned error, count %d" bs.error_count)
    in
    if bs.error_count < c.config.error_threshold then return bs
    else
      let* () =
        Logs_lwt.warn (fun l ->
            l "Error threshold exceeded (%d of %d)" bs.error_count
              c.config.error_threshold)
      in
      (* Send RDY 0 and send retry trial command after a delay  *)
      let* () = Logs_lwt.debug (fun l -> l "Breaker open, sending RDY 0") in
      let* () = send ~timeout:c.config.write_timeout ~conn (RDY 0) in
      let bs = { bs with position = Open } in
      let duration =
        backoff_duration ~multiplier:c.config.backoff_multiplier
          ~error_count:bs.error_count
      in
      do_after_async ~duration (fun () -> Lwt_mvar.put mbox TrialBreaker);
      return bs

  let update_breaker_state c conn open_breaker cmd bs =
    match cmd with
    | NOP -> return bs (* Receiving a NOP should not alter our state  *)
    | _ -> (
        let error_state = match cmd with REQ _ -> `Error | _ -> `Ok in
        match (error_state, bs.position) with
        | `Error, Closed -> open_breaker bs
        | `Error, Open -> return bs
        | `Error, HalfOpen ->
            (* Failed test *)
            open_breaker bs
        | `Ok, Closed -> return bs
        | `Ok, Open -> return bs
        | `Ok, HalfOpen ->
            (* Passed test  *)
            let rdy = rdy_per_connection c in
            let* () =
              Logs_lwt.debug (fun l ->
                  l "%s Trial passed, sending RDY %d" c.log_prefix rdy)
            in
            let* () = send ~timeout:c.config.write_timeout ~conn (RDY rdy) in
            return { position = Closed; error_count = 0 } )

  let consume c conn mbox =
    let maybe_open_breaker = maybe_open_breaker c conn mbox in
    let update_breaker_state = update_breaker_state c conn maybe_open_breaker in
    let send = send ~timeout:c.config.write_timeout ~conn in
    let rec mbox_loop bs =
      let* taken = Lwt_mvar.take mbox in
      match taken with
      | RawFrame raw -> (
          match ServerMessage.of_raw_frame raw with
          | Ok server_message ->
              async (fun () ->
                  let* resp =
                    handle_server_message server_message c.handler
                      c.config.max_attempts
                  in
                  match resp with
                  | Some c -> Lwt_mvar.put mbox (Command c)
                  | None -> return_unit);
              mbox_loop bs
          | Error s ->
              let* () =
                Logs_lwt.err (fun l ->
                    l "%s Error parsing response: %s" c.log_prefix s)
              in
              mbox_loop bs )
      | Command cmd ->
          let* () = send cmd in
          let* new_state = update_breaker_state cmd bs in
          mbox_loop new_state
      | TrialBreaker ->
          let* () =
            Logs_lwt.debug (fun l ->
                l "%s Breaker trial, sending RDY 1 (Error count: %i)"
                  c.log_prefix bs.error_count)
          in
          let bs = { bs with position = HalfOpen } in
          let* () = send (RDY 1) in
          mbox_loop bs
      | ConnectionError s -> failwith s
      | RecalcRDY -> (
          (* Only recalc and send if breaker is closed  *)
          match bs.position with
          | Open -> mbox_loop bs
          | HalfOpen -> mbox_loop bs
          | Closed ->
              let rdy = rdy_per_connection c in
              let* () =
                Logs_lwt.debug (fun l ->
                    l "%s Sending recalculated RDY %d" c.log_prefix rdy)
              in
              let* () = send (RDY rdy) in
              mbox_loop bs )
    in
    let* () = send MAGIC in
    let ic = Config.to_identity_config c.config in
    let* () =
      identify ~read_timeout:c.config.read_timeout
        ~write_timeout:c.config.write_timeout ~conn ic
    in
    let* () =
      subscribe ~read_timeout:c.config.read_timeout
        ~write_timeout:c.config.write_timeout ~conn c.topic c.channel
    in
    (* Start cautiously by sending RDY 1 *)
    let* () =
      Logs_lwt.debug (fun l -> l "%s Sending initial RDY 1" c.log_prefix)
    in
    let* () = send (RDY 1) in
    (* Start background reader *)
    async (fun () -> read_loop ~timeout:c.config.read_timeout conn mbox);
    let initial_state = { position = HalfOpen; error_count = 0 } in
    mbox_loop initial_state

  let connection_loop c address mbox =
    let rec loop error_count =
      let* conn =
        catch_result (fun () -> connect address c.config.dial_timeout)
      in
      match conn with
      | Ok conn ->
          let handle_exn e =
            Lwt.join
              [
                Logs_lwt.err (fun l -> l "Consumer error: %s" (Exn.to_string e));
                Lwt_io.close (fst conn);
                Lwt_io.close (snd conn);
                Lwt_unix.sleep default_backoff;
              ]
          in
          Hash_set.add c.open_connections address;
          let* () =
            Logs_lwt.debug (fun l ->
                l "%s %d connections" c.log_prefix
                  (Hash_set.length c.open_connections))
          in
          let* () = catch (fun () -> consume c conn mbox) handle_exn in
          (* Error consuming. If we get here it means that something failed and we need to reconnect *)
          Hash_set.remove c.open_connections address;
          let* () =
            Logs_lwt.debug (fun l ->
                l "%s %d connections" c.log_prefix
                  (Hash_set.length c.open_connections))
          in
          let error_count = 1 in
          loop error_count
      | Error e ->
          (* Error connecting *)
          let* () =
            Logs_lwt.err (fun l ->
                l "%s Connecting to consumer '%s': %s" c.log_prefix
                  (Address.to_string address)
                  e)
          in
          let error_count = error_count + 1 in
          let stop_connecting =
            match c.mode with
            | `Nsqd -> false
            | `Lookupd -> error_count > lookupd_error_threshold
          in
          if stop_connecting then
            (* Log and stop recursing *)
            Logs_lwt.err (fun l ->
                l "%s Exceeded reconnection threshold (%d), not reconnecting"
                  c.log_prefix error_count)
          else
            let duration =
              backoff_duration ~multiplier:default_backoff ~error_count
            in
            let* () =
              Logs_lwt.debug (fun l ->
                  l "%s Sleeping for %f seconds" c.log_prefix
                    (Seconds.value duration))
            in
            let* () = Lwt_unix.sleep (Seconds.value duration) in
            loop error_count
    in
    loop 0

  (** 
    Start a thread to update RDY count occasionaly.
    The number of open connections can change as we add new consumers due to lookupd
    discovering producers or we have connection failures. Each time a new connection 
    is opened or closed the consumer.nsqd_connections field is updated.
    We therefore need to occasionaly update our RDY count as this may have changed so that
    it is spread evenly across connections.
  *)
  let start_ready_calculator c mbox =
    let jitter = Random.float (recalculate_rdy_interval /. 10.0) in
    let interval = recalculate_rdy_interval +. jitter in
    let switch = Lwt_switch.create () in
    let rec loop () =
      if not (Lwt_switch.is_on switch) then return_unit
      else
        let* () = Lwt_unix.sleep interval in
        let* () =
          Logs_lwt.debug (fun l -> l "%s recalculating RDY" c.log_prefix)
        in
        let* () = Lwt_mvar.put mbox RecalcRDY in
        loop ()
    in
    (* Start loop in background *)
    async (fun () -> loop ());
    (* Return a func to flip switch and cancel the loop *)
    fun () -> Lwt_switch.turn_off switch

  let start_nsqd_consumer c address =
    let mbox = Lwt_mvar.create_empty () in
    let cancel = start_ready_calculator c mbox in
    let* () = connection_loop c address mbox in
    (* We've given up on the connection loop, clean up *)
    cancel ()

  let start_polling_lookupd c =
    let poll_interval =
      Float.(
        (1.0 + Random.float c.config.lookupd_poll_jitter)
        * Seconds.value c.config.lookupd_poll_interval)
    in
    let rec check_for_producers () =
      catch
        (fun () ->
          let* () =
            Logs_lwt.debug (fun l ->
                l "Querying %d lookupd hosts" (List.length c.addresses))
          in
          let* results =
            Lwt_list.map_p (query_nsqlookupd ~topic:c.topic) c.addresses
          in
          let discovered_producers =
            List.filter_map ~f:Result.ok results
            |> List.map ~f:Lookup.producer_addresses
            |> List.join
            |> Hash_set.of_list (module Address)
          in
          let running = c.open_connections in
          let new_producers = Hash_set.diff discovered_producers running in
          let* () =
            Logs_lwt.debug (fun l ->
                l "Found %d new producers" (Hash_set.length new_producers))
          in
          Hash_set.iter
            ~f:(fun a ->
              async (fun () ->
                  let* () =
                    Logs_lwt.debug (fun l ->
                        l "Starting consumer for: %s" (Address.to_string a))
                  in
                  start_nsqd_consumer c a))
            new_producers;
          let* () = Lwt_unix.sleep poll_interval in
          check_for_producers ())
        (fun e ->
          let* () =
            Logs_lwt.err (fun l ->
                l "Error polling lookupd: %s" (Exn.to_string e))
          in
          let* () = Lwt_unix.sleep default_backoff in
          check_for_producers ())
    in
    check_for_producers ()

  let run c =
    match c.mode with
    | `Lookupd ->
        let* () = Logs_lwt.debug (fun l -> l "Starting lookupd poller") in
        start_polling_lookupd c
    | `Nsqd ->
        let consumers =
          List.map ~f:(fun a -> start_nsqd_consumer c a) c.addresses
        in
        join consumers
end

module Producer = struct
  type connection = {
    conn : Lwt_io.input Lwt_io.channel * Lwt_io.output Lwt_io.channel;
    mutable last_write : float;
  }

  type t = { address : Address.t; pool : connection Lwt_pool.t }

  let default_pool_size = 5

  let default_dial_timeout = Seconds.of_float 15.0

  let default_write_timeout = Seconds.of_float 15.0

  let default_read_timeout = Seconds.of_float 15.0

  (** Throw away connections that are idle for this long
       Note that NSQ expects hearbeats to be answered every 30 seconds
       and if two are missed it closes the connection.
  *)
  let ttl_seconds = 50.0

  let create_pool address size =
    let validate c =
      let now = Unix.time () in
      let diff = now -. c.last_write in
      return Float.(diff < ttl_seconds)
    in
    (* Always return false so that we throw away connections where we encountered an error *)
    let check _ is_ok = is_ok false in
    let dispose c =
      let* () =
        Logs_lwt.warn (fun l -> l "Error publishing, closing connection")
      in
      join [ Lwt_io.close (fst c.conn); Lwt_io.close (snd c.conn) ]
    in
    Lwt_pool.create size ~validate ~check ~dispose (fun () ->
        let* conn = connect address default_dial_timeout in
        let* () = send ~timeout:default_write_timeout ~conn MAGIC in
        let last_write = Unix.time () in
        return { conn; last_write })

  let create ?(pool_size = default_pool_size) address =
    if pool_size < 1 then Error "Pool size must be >= 1"
    else Ok { address; pool = create_pool address pool_size }

  let publish_cmd t cmd =
    let with_conn c =
      let rec read_until_ok () =
        let* frame = read_raw_frame ~timeout:default_read_timeout c.conn in
        match ServerMessage.of_raw_frame frame with
        | Ok ResponseOk -> return_ok ()
        | Ok Heartbeat ->
            let* () = send ~timeout:default_write_timeout ~conn:c.conn NOP in
            c.last_write <- Unix.time ();
            read_until_ok ()
        | Ok _ -> return_error "Expected OK or Heartbeat, got another message"
        | Error e -> return_error (Printf.sprintf "Received error: %s" e)
      in
      let* () = send ~timeout:default_write_timeout ~conn:c.conn cmd in
      c.last_write <- Unix.time ();
      read_until_ok ()
    in
    let try_publish () = Lwt_pool.use t.pool with_conn in
    let handle_ex e =
      let message =
        Printf.sprintf "Publishing to `%s`: %s"
          (Address.to_string t.address)
          (Exn.to_string e)
      in
      return_error message
    in
    catch try_publish handle_ex

  let publish t topic message =
    let cmd = PUB (topic, message) in
    publish_cmd t cmd

  let publish_multi t topic messages =
    let cmd = MPUB (topic, messages) in
    publish_cmd t cmd
end
