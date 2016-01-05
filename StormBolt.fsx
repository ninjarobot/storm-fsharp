#load "StormIO.fsx"

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
    try 
        let doc = jsonDeserialize <| line
        logToStorm "Received input, about to process."
        match doc |> isHeartbeat with
        | true -> syncToStorm ()
        | false ->
            match doc.TryGetValue("id") with
            | true, idToken -> 
                match idToken with 
                | :? JValue as jval ->
                    let tupleId = jval.Value :?> string
                    match doc.TryGetValue("tuple") with
                    | true, tupleToken ->
                        match tupleToken with
                        | :? JValue as jval ->
                            let tuple = jval.Value :?> obj array
                            let word = tuple.[0] :?> string
                            emitToStorm[|word; word.Length|]
                            ackToStorm tupleId
                        | _ -> failwith "Missing tuple data."
                    | _ -> failwith "Missing tuple element."
                | _ -> 
                    failwith "Invalid tuple id."
            | false, _ -> 
                failwith (sprintf "Message missing id.: %s" line)
    with
    | _ as ex -> log.WarnFormat ("Input from storm ignored: '{0}'", line, ex)
    input |> processInput

let line = fromStorm <| stdin
let config = jsonDeserialize <| line
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
