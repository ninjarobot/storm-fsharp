#load "StormIO.fsx"
#load "Logging.fsx"

open System.IO
open Newtonsoft.Json.Linq
open StormIO

let rec processInput (input:TextReader) =
    let line = fromStorm <| input
    let deserialized = jsonDeserialize <| line
    logToStorm "Received input, about to process."
    match deserialized with
    | Some doc ->
        match doc.TryGetValue("command") with
        | true, token -> 
            match token with 
            | :? JValue as jval ->
                match jval.Value :?> string with
                | "next" -> 
                    logToStorm "Emitting tuples."
                    let tuples = [|"some";"test";"data";"from";"F#"|] 
                    tuples |> Array.iter(fun s -> emitToStorm [|s|])
                    sprintf "Emitted %i tuples." tuples.Length |> logToStorm
                | _ -> 
                    failwith "Got unknown command."
            | _ -> 
                failwith "Got non-string commmand value."
        | false, _ -> 
            failwith (sprintf "Message missing command.: %s" line)
        System.Threading.Thread.Sleep (1)
        syncToStorm ()
    | None -> sprintf "Input from storm ignored: '%s'" line |> Logging.Warn
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
