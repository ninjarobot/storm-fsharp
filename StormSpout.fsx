#load "StormIO.fsx"

open System.IO
open Newtonsoft.Json.Linq
open StormIO

let rec processInput (input:TextReader) =
    let line = fromStorm <| input
    try 
        let doc = jsonDeserialize <| line
        logToStorm "Received input, about to process."
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
