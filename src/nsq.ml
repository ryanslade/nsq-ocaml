open Containers
open Printf
open Lwt

module Milliseconds = struct
  type t = Milliseconds of int64

  let of_int64 i =
    Milliseconds i

  let value (Milliseconds i) = i
end

module MessageID = struct
  type t =
      MessageID of bytes

  let of_bytes b =
    MessageID b

  let to_string = function
    | MessageID id -> Bytes.to_string id
end

let magic = "  V2"
let ephemeral_suffix = "#ephemeral"
let default_backoff_seconds = 1.0
let default_requeue_delay = Milliseconds.of_int64 5000L
let max_backoff_seconds = 3600.0

type raw_frame = {
  size: int32;
  frame_type: int32;
  data: bytes; 
}

type raw_message = {
  timestamp: int64;
  attempts: Unsigned.UInt16.t;
  id: MessageID.t;
  body: bytes; 
}

type address =
  | Host of string
  | HostPort of string * int

let make_ephemeral s =
  s ^ ephemeral_suffix

module Topic = struct
  type t =
    | Topic of string
    | TopicEphemeral of string

  let to_string = function
    | Topic s -> s
    | TopicEphemeral s -> make_ephemeral s
end

module Channel = struct 
  type t =
    | Channel of string
    | ChannelEphemeral of string

  let to_string = function
    | Channel s -> s
    | ChannelEphemeral s -> make_ephemeral s 
end

module FrameType = struct
  type t =
    | FrameResponse
    | FrameError
    | FrameMessage
    | FrameUnknown of int32

  let of_int32 = function
    | 0l -> FrameResponse
    | 1l -> FrameError
    | 2l -> FrameMessage
    | i -> FrameUnknown i
end

type command =
  (* TODO: Identify *)
  | Magic
  | SUB of Topic.t * Channel.t
  | PUB of Topic.t * bytes
  | MPUB of Topic.t * bytes list
  | REQ of MessageID.t * Milliseconds.t
  | FIN of MessageID.t
  | TOUCH of MessageID.t
  | RDY of int
  | AUTH of string
  | CLS
  | NOP

type server_message =
  | ResponseOk
  | Heartbeat
  | ErrorInvalid of string
  | ErrorBadTopic of string
  | ErrorBadChannel of string
  | ErrorFINFailed of string
  | ErrorREQFailed of string
  | ErrorTOUCHFailed of string
  | Message of raw_message

type handler_result =
  | HandlerOK
  | HandlerRequeue

let string_of_pub t data =
  let ts = Topic.to_string t in
  let buf = Bytes.create 4 in
  EndianBytes.BigEndian.set_int32 buf 0 (Int32.of_int @@ Bytes.length data);
  Format.sprintf "PUB %s\n%s%s" ts (Bytes.to_string buf) (Bytes.to_string data)

(** 
   MPUB <topic_name>\n
   [ 4-byte body size ]
   [ 4-byte num messages ]
   [ 4-byte message #1 size ][ N-byte binary data ]
      ... (repeated <num_messages> times)

   <topic_name> - a valid string (optionally having #ephemeral suffix)
*)
let string_of_mpub t bodies =
  let ts = Topic.to_string t in
  let body_count = List.length bodies in
  let data_size = List.fold_left (fun a b -> a + Bytes.length b) 0 bodies in
  let buf = Bytes.create (4 + 4 + (4*body_count) + data_size) in
  EndianBytes.BigEndian.set_int32 buf 0 (Int32.of_int @@ data_size);
  EndianBytes.BigEndian.set_int32 buf 4 (Int32.of_int body_count);
  let index = ref 8 in
  List.iter (
    fun b -> 
      EndianBytes.BigEndian.set_int32 buf !index (Int32.of_int @@ Bytes.length b);
      index := !index + 4;
      Bytes.blit b 0 buf !index (Bytes.length b);
      index := !index + Bytes.length b;
  ) bodies;
  Format.sprintf "MPUB %s\n%s" ts (Bytes.to_string buf)

(* TODO: Return bytes instead? *)
let string_of_command = function
  | Magic -> magic 
  | NOP -> "NOP\n"
  | RDY i -> sprintf "RDY %i\n" i
  | FIN id -> sprintf "FIN %s\n" (MessageID.to_string id)
  | TOUCH id -> sprintf "TOUCH %s\n" (MessageID.to_string id)
  | SUB (t, c) -> sprintf "SUB %s %s\n" (Topic.to_string t) (Channel.to_string c)
  | REQ (id, delay) -> sprintf "REQ %s %Li\n" (MessageID.to_string id) (Milliseconds.value delay)
  | PUB (t, data) -> string_of_pub t data
  | MPUB (t, data) -> string_of_mpub t data
  | CLS -> "CLS\n"
  | AUTH secret -> 
    let buf = Bytes.create 4 in
    EndianBytes.BigEndian.set_int32 buf 0 (Int32.of_int (String.length secret));
    Format.sprintf "AUTH\n%s%s" (Bytes.to_string buf) secret

let send command (_, oc) =
  string_of_command command 
  |> Lwt_io.write oc

let catch_result promise =
  catch
    (fun () -> promise >>= fun x -> return (Result.Ok x))
    (fun e -> return (Result.Error e))

let connect host =
  let (host, port) = match host with
    | Host h          -> (h, 4150)
    | HostPort (h, p) -> (h, p) in
  let host = Unix.inet_addr_of_string host in
  let addr = Unix.ADDR_INET(host, port) in
  Lwt_io.open_connection addr 

let read_raw_frame (ic, _) =
  Lwt_io.BE.read_int32 ic >>= fun size ->
  Lwt_io.BE.read_int32 ic >>= fun frame_type ->
  let data_size = (Int32.to_int size) - 4 in
  let data = Bytes.create data_size in
  Lwt_io.read_into_exactly ic data 0 data_size >>= fun () ->
  return {size; frame_type; data}

let parse_response_body body =
  match (Bytes.to_string body) with
  | "OK" -> Result.Ok ResponseOk
  | "_heartbeat_" -> Result.Ok Heartbeat
  | _ -> Result.Error (sprintf "Unknown response: %s" (Bytes.to_string body))

let parse_message_body_exn body =
  let timestamp = EndianBytes.BigEndian.get_int64 body 0 in
  let attempts = EndianBytes.BigEndian.get_uint16 body 8 |> Unsigned.UInt16.of_int in
  let length = Bytes.length body in
  let id = MessageID.of_bytes (Bytes.sub body 10 16) in
  let body = Bytes.sub body 26 (length-26) in
  Message { timestamp; attempts; id; body; }

let parse_message_body body =
  Result.guard_str @@ fun () -> parse_message_body_exn body

let parse_error_body body =
  match String.Split.left ~by:" " (Bytes.to_string body) with
  | None -> Result.Error (sprintf "Malformed error code: %s" (Bytes.to_string body))
  | Some (code, detail) ->
    match code with
    | "E_INVALID" -> Result.return @@ ErrorInvalid detail
    | "E_BAD_TOPIC" -> Result.return @@ ErrorBadTopic detail
    | "E_BAD_CHANNEL" -> Result.return @@ ErrorBadChannel detail
    | "E_FIN_FAILED" -> Result.return @@ ErrorFINFailed detail
    | _ -> Result.Error (sprintf "Unknown error code: %s. %s" code detail)

let parse_raw_frame raw =
  match (FrameType.of_int32 raw.frame_type) with
  | FrameResponse -> parse_response_body raw.data
  | FrameMessage -> parse_message_body raw.data
  | FrameError -> parse_error_body raw.data
  | FrameUnknown i -> Result.Error (sprintf "Unknown frame type: %li" i)

let subscribe topic channel conn =
  send (SUB (topic, channel)) conn >>= fun () ->
  read_raw_frame conn >>= fun raw ->
  match parse_raw_frame raw with
  | Result.Ok ResponseOk -> return_unit
  | _ -> fail_with "Unexpected SUB response"

let requeue_delay attempts =
  let d = Milliseconds.value default_requeue_delay in
  let attempts = Int64.of_int_exn attempts in
  Milliseconds.of_int64 Int64.(d * attempts)

let handle_message handler msg max_attempts =
  catch_result @@ handler msg.body >>=
  begin
    function
    | Result.Ok r -> return r
    | Result.Error e ->
      Lwt_log.error_f "Handler error: %s" (Printexc.to_string e) >>= fun () ->
      return HandlerRequeue
  end
  >>= function 
  | HandlerOK -> return (FIN msg.id)
  | HandlerRequeue -> 
    let attempts = Unsigned.UInt16.to_int msg.attempts in
    if attempts >= max_attempts
    then 
      return (FIN msg.id)
    else
      let delay = requeue_delay attempts in
      return (REQ (msg.id, delay))

let handle_frame frame handler max_attempts =
  let warn_return_none name msg = Lwt_log.warning_f "%s: %s" name msg >>= fun () -> return_none in
  match frame with
  | ResponseOk -> return_none
  | Heartbeat -> return_some NOP
  | ErrorInvalid s ->  warn_return_none "ErrorInvalid" s
  | ErrorBadTopic s -> warn_return_none "ErrorBadTopic" s 
  | ErrorBadChannel s -> warn_return_none "ErrorBadChannel" s 
  | ErrorFINFailed s -> warn_return_none "ErrorFINFailed" s 
  | ErrorREQFailed s -> warn_return_none "ErrorREQFailed" s 
  | ErrorTOUCHFailed s -> warn_return_none "ErrorTOUCHFailed" s 
  | Message msg -> handle_message handler msg max_attempts >>= fun cmd -> return_some cmd

module Consumer = struct
  type config = {
    max_in_flight : int;
    max_attempts : int;
  }

  let validate_positive c value name =
    if value > 0
    then Result.Ok c
    else Result.Error (Format.sprintf "%s must be greater than 0" name)

  let max_in_flight_positive c =
    validate_positive c c.max_in_flight "max_in_flight"

  let max_attempts_positive c =
    validate_positive c c.max_attempts "max_attempts"

  let max_attempts_less_than max c =
    if c.max_attempts < max
    then Result.Ok c
    else Result.Error (Format.sprintf "max_attempts must be less than %d, got %d" max c.max_attempts)

  let validate_config c =
    let open Result in
    c |> max_in_flight_positive
    >>= max_attempts_positive
    >>= max_attempts_less_than 65535

  let default_config = {
    max_in_flight = 1;
    max_attempts = 5;
  }

  type breaker_position =
    | Closed
    | HalfOpen
    | Open

  type breaker_state = {
    position : breaker_position;
    error_count : int;
  }

  let backoff_multiplier = 0.5

  (* TODO: This should be configurable *)
  let backoff_duration error_count =
    let bo = (backoff_multiplier *. (float_of_int error_count)) in
    min bo max_backoff_seconds

  type t = {
    addresses : address list;
    desired_rdy : int;
    topic : Topic.t;
    channel : Channel.t;
    handler : (bytes -> handler_result Lwt.t);
    config : config;
  }

  let create ?(config=default_config) addresses topic channel handler =
    match validate_config config with
    | Result.Error s -> Result.Error (Format.sprintf "Invalid config: %s" s)
    | Result.Ok config ->
      let desired_rdy = max 1 (config.max_in_flight / (List.length addresses)) in
      Result.return { 
        addresses; 
        desired_rdy;
        topic; 
        channel; 
        handler;
        config;
      }

  let after duration f =
    Lwt_log.debug_f "Sleeping for %f seconds" duration >>= fun () ->
    Lwt_unix.sleep duration >>= fun () ->
    Lwt_log.debug "Sleeping done" >>= fun () ->
    f ();

  type mailbox_message =
    | RawFrame of raw_frame
    | Command of command
    | TrialBreaker
    | ConnectionError of exn

  let rec read_loop conn mbox =
    let put_async m = Lwt.async @@ fun () -> Lwt_mvar.put mbox m in
    catch_result @@ read_raw_frame conn >>= function
    | Result.Ok raw ->
      put_async @@ RawFrame raw;
      read_loop conn mbox
    | Result.Error e ->
      put_async @@ ConnectionError e;
      return_unit

  let consume c conn mbox =
    send Magic conn >>= fun () ->
    subscribe c.topic c.channel conn >>= fun () ->
    (* Start cautiously by sending RDY 1 *)
    send (RDY 1) conn >>= fun () ->
    Lwt_log.debug "Sending initial RDY 1" >>= fun () ->
    (* Start background reader *)
    Lwt.async (fun () -> read_loop conn mbox);
    let open_breaker bs =
      (* Send RDY 0 and send retry trial command after a delay  *)
      ignore_result @@ Lwt_log.debug "Breaker open, sending RDY 0";
      ignore_result @@ send (RDY 0) conn;
      let bs = { error_count = bs.error_count + 1; position = Open } in
      let duration = backoff_duration bs.error_count in
      ignore_result @@ after duration (fun () -> Lwt_mvar.put mbox TrialBreaker);
      bs
    in
    let rec mbox_loop bs =
      Lwt_mvar.take mbox >>= function
      | RawFrame raw ->
        begin 
          match parse_raw_frame raw with
          | Result.Ok frame ->
            Lwt.async 
              begin
                fun () -> 
                  handle_frame frame c.handler c.config.max_attempts >>= function
                  | Some c -> Lwt_mvar.put mbox (Command c)
                  | None -> return_unit
              end;
            mbox_loop bs

          | Result.Error s -> 
            Lwt_log.error_f "Error parsing response: %s" s >>= fun () ->
            mbox_loop bs
        end
      | Command cmd ->
        send cmd conn >>= fun () ->
        let is_error = match cmd with
          | REQ _ -> true
          | _ -> false 
        in
        (* Backoff logic *)
        let bs = match is_error, bs.position with
          | true, Closed -> 
            open_breaker bs
          | true, Open -> bs
          | true, HalfOpen ->
            (* Failed test *)
            open_breaker bs
          | false, Closed -> bs
          | false, Open -> bs
          | false, HalfOpen ->
            (* Passed test  *)
            ignore_result @@ send (RDY c.desired_rdy) conn;
            { position = Closed; error_count = 0; }
        in
        mbox_loop bs
      | TrialBreaker ->
        ignore_result @@ Lwt_log.debug_f "Breaker trial, sending RDY 1 (Error count: %i)" bs.error_count;
        let bs = { bs with position = HalfOpen } in
        ignore_result @@ send (RDY 1) conn;
        mbox_loop bs
      | ConnectionError e ->
        fail e 
    in
    let bs = { position = HalfOpen; error_count = 0; } in
    mbox_loop bs

  let rec main_loop c address mbox =
    catch_result @@ connect address >>= function
    | Result.Ok conn ->
      let handle_ok () = consume c conn mbox in
      let handle_ex e = 
        Lwt_io.close (fst conn) >>= fun () ->
        Lwt_io.close (snd conn) >>= fun () ->
        Lwt_log.error_f "Reader failed: %s" (Printexc.to_string e) >>= fun () ->
        Lwt_unix.sleep default_backoff_seconds
      in
      catch handle_ok handle_ex >>= fun () ->
      main_loop c address mbox
    | Result.Error e ->
      Lwt_log.error_f "Error connecting: %s" (Printexc.to_string e) >>= fun () ->
      Lwt_unix.sleep default_backoff_seconds >>= fun () ->
      main_loop c address mbox

  let async_exception_hook e =
    ignore_result @@ Lwt_log.error_f "Async exception: %s" (Printexc.to_string e)

  let start_consumer c address =
    let mbox = Lwt_mvar.create_empty () in
    main_loop c address mbox

  let run c =
    Lwt.async_exception_hook := async_exception_hook;
    let readers = List.map (fun a -> start_consumer c a) c.addresses in
    Lwt.join readers

end

module Publisher = struct
  type connection = {
    conn : (Lwt_io.input Lwt_io.channel * Lwt_io.output Lwt_io.channel);
    last_send : float ref
  }

  type t = { 
    pool : connection Lwt_pool.t
  }

  let default_pool_size = 5

  (** Throw away connections that are idle for this long
       Note that NSQ expects hearbeats to be answered every 30 seconds
       and if two are missed it closes the connection.
  *)
  let ttl_seconds = 50.0 

  let create_pool address size =
    let validate = fun c ->
      let now = Unix.time () in
      let diff = now -. !(c.last_send) in
      return (diff < ttl_seconds)
    in
    Lwt_pool.create size ~validate
      begin
        fun () -> 
          connect address >>= fun conn ->
          send Magic conn >>= fun () ->
          let last_send = ref (Unix.time ()) in
          return { conn; last_send }
      end

  let create ?(pool_size=default_pool_size) address = 
    if pool_size <= 0
    then Result.Error "Pool size must be >= 1"
    else Result.Ok { pool = create_pool address pool_size }

  let publish_cmd t topic cmd =
    let with_conn c =
      let rec read_until_ok () =
        read_raw_frame c.conn >>= fun raw ->
        match parse_raw_frame raw with
        | Result.Ok ResponseOk -> return @@ Result.Ok ()
        | Result.Ok Heartbeat -> 
          send NOP c.conn >>= fun () -> 
          c.last_send := Unix.time ();
          read_until_ok ()
        | Result.Ok _ -> return (Result.Error "Expected OK or Heartbeat, got another message") 
        | Result.Error e -> return (Result.Error (sprintf "Received error: %s" e))
      in
      send cmd c.conn >>= fun () ->
      c.last_send := Unix.time ();
      read_until_ok ()
    in
    let try_publish () = Lwt_pool.use t.pool with_conn in
    let handle_ex e =
      let message = sprintf "Error publishing: %s" (Printexc.to_string e) in
      return (Result.Error message)
    in
    catch try_publish handle_ex

  let publish t topic message =
    let cmd = (PUB (topic, message)) in
    publish_cmd t topic cmd

  let publish_multi t topic messages =
    let cmd = (MPUB (topic, messages)) in
    publish_cmd t topic cmd

end
