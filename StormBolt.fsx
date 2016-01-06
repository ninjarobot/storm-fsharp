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
    log.InfoFormat("Bolt line: {0}", line)
    try 
        let doc = jsonDeserialize <| line
        logToStorm "Received input, about to process."
        match doc |> isHeartbeat with
        | true ->
            "Received heartbeat." |> logToStorm
            syncToStorm ()
        | false ->
            "Received tuple" |> logToStorm
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
                            sprintf "Got tuple %s" word |> logToStorm
                            log.InfoFormat("Got tuple: {0}", word)
                            emitToStorm[|word; word.Length|]
                            log.InfoFormat("Emitted tuple")
                            ackToStorm tupleId
                            log.InfoFormat("Ack'd tuple with ID {0}.", tupleId)
                            "Emitted tuple with length" |> logToStorm
                        | _ -> failwith "Missing tuple data."
                    | _ -> failwith "Missing tuple element."
                | _ ->
                    "Invalid tuple id" |> logToStorm 
                    //failwith "Invalid tuple id."
                    log.ErrorFormat("Invalid tuple id.")
            | false, _ -> 
                log.ErrorFormat("Missing id.")
                failwith (sprintf "Message missing id.: %s" line)
    with
    | _ as ex -> log.WarnFormat ("Input from storm ignored: {0} {1}", line, ex)
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
