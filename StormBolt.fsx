#load "StormIO.fsx"
#load "Logging.fsx"

open System.IO
open Newtonsoft.Json.Linq
open StormIO

let isHeartbeat (json:JObject) =
    match json.TryGetValue("stream") with
    | true, streamToken ->
        match streamToken with
        | :? JValue as jval ->
            match jval.Value :?> string with
            | "__heartbeat" -> true
            | _ -> false
        | _ -> false
    | _ -> false

let rec processInput (input:TextReader) =
    let line = fromStorm <| input
    sprintf "Bolt line: '%s'" line |> Logging.Debug
    let deserialized = jsonDeserialize <| line
    match deserialized with 
    | Some doc ->
        "Received input, about to process." |> Logging.Debug
        match doc |> isHeartbeat with
        | true ->
            "Received heartbeat." |> Logging.Debug
            syncToStorm ()
        | false ->
            "Received tuple" |> Logging.Debug
            match doc.TryGetValue("id") with
            | true, idToken -> 
                match idToken with 
                | :? JValue as jval ->
                    let tupleId = jval.Value :?> string
                    match doc.TryGetValue("tuple") with
                    | true, tupleToken ->
                        match tupleToken with
                        | :? JArray as jarr ->
                            let tuple = jarr.ToObject<obj[]>()
                            let word = tuple.[0] :?> string
                            emitToStorm[|word; word.Length|]
                            ackToStorm tupleId
                        | _ -> failwith "Missing tuple data."
                    | _ -> failwith "Missing tuple element."
                | _ ->
                    "Invalid tuple id" |> logToStorm 
            | false, _ -> 
                "Missing id." |> Logging.Error
                failwith (sprintf "Message missing id.: %s" line)
    | None -> sprintf "Malformed input from storm ignored: '%s'" line |> Logging.Warn
    input |> processInput

let line = fromStorm <| stdin
let deserialized = jsonDeserialize <| line
match deserialized with
| Some config ->
    match config.TryGetValue("pidDir") with 
    | true, token ->
        match token with
        | :? JValue as jval ->
            let pidDir = jval.Value :?> string
            let pid = System.Diagnostics.Process.GetCurrentProcess().Id
            use fs = File.Create(Path.Combine(pidDir, pid.ToString()))
            fs.Close()
            { Pid.pid=pid } |> toStorm
        | _ -> failwith "pidDir had no value."
    | _ -> failwith "Missing pidDir in initial handshake."
    stdin |> processInput |> ignore
| None -> failwith (sprintf "Invalid handshake: %s" line)
