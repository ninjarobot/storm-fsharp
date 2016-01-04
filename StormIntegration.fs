module StormIntegration

open System.IO
open Newtonsoft.Json
open Newtonsoft.Json.Linq

type Pid = { pid:int }
type Emit = { command:string; id:string; stream:int; task:int; tuple:obj array }
let EmitDoc = { Emit.command="emit"; id=null; stream=0; task=0; tuple=[||] }
type Log = { command:string; msg:string }
let LogDoc = { Log.command="log"; msg=null }
type Sync = { command:string }
let SyncDoc = { Sync.command="sync" }

log4net.Config.XmlConfigurator.Configure() |> ignore
let log = log4net.LogManager.GetLogger("StormIntegration")

let jsonSerialize o =
    JsonConvert.SerializeObject (o, Formatting.None, JsonSerializerSettings (DefaultValueHandling=DefaultValueHandling.Ignore))

let jsonDeserialize s = JObject.Parse s

let toStorm (o:obj) =
    o |> jsonSerialize |> stdout.WriteLine
    "end" |> stdout.WriteLine
    stdout.Flush()

let fromStorm (input:TextReader) =
    let rec getMessage (sb:System.Text.StringBuilder) =
        match input.ReadLine() with
        | "end" -> sb.ToString()
        | s ->
            sb.AppendLine(s) |> getMessage
    System.Text.StringBuilder() |> getMessage

let emitToStorm (tuple:obj array) =
    { EmitDoc with tuple=tuple } |> toStorm

let logToStorm msg =
    { LogDoc with msg = msg } |> toStorm

let syncToStorm () =
    SyncDoc |> toStorm

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

[<EntryPoint>]
let main argv =
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
    stdin |> processInput
    0
