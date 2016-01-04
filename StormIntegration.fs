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

let sendStorm (o:obj) =
    let os = stdout
    o |> jsonSerialize |> os.WriteLine
    "end" |> os.WriteLine
    os.Flush()

let recvStorm (input:TextReader) =
    let rec getMessage (sb:System.Text.StringBuilder) =
        match input.ReadLine() with
        | "end" -> sb.ToString()
        | s ->
            sb.AppendLine(s) |> getMessage
    System.Text.StringBuilder() |> getMessage

let writeEmit (tuple:obj array) =
    { EmitDoc with tuple=tuple } |> sendStorm

let writeLog msg =
    { LogDoc with msg = msg } |> sendStorm

let writeSync () =
    SyncDoc |> sendStorm

let rec readmore (input:TextReader) =
    let line = input |> recvStorm 
    try 
        let doc = jsonDeserialize <| line
        writeLog "Received input, about to process."
        match doc.TryGetValue("command") with
        | true, token -> 
            match token with 
            | :? JValue as jval ->
                match jval.Value :?> string with
                | "next" -> 
                    writeLog "Emitting tuples."
                    let tuples = [|"some";"test";"data";"from";"F#"|] 
                    tuples |> Array.iter(fun s -> writeEmit [|s|])
                    sprintf "Emitted %i tuples." tuples.Length |> writeLog
                | _ -> 
                    failwith "Got unknown command."
            | _ -> 
                failwith "Got non-string commmand value."
        | false, _ -> 
            failwith (sprintf "Message missing command.: %s" line)
        System.Threading.Thread.Sleep (1)
        writeSync ()
    with
    | _ as ex -> log.WarnFormat ("Input from storm ignored: '{0}'", line, ex)
    input |> readmore

[<EntryPoint>]
let main argv =
    let line = stdin |> recvStorm
    let config = jsonDeserialize <| line
    match config.TryGetValue("pidDir") with 
    | true, token ->
        match token with
        | :? JValue as jval ->
            let pidDir = jval.Value :?> string
            let pid = System.Diagnostics.Process.GetCurrentProcess().Id
            use fs = File.Create(Path.Combine(pidDir, pid.ToString()))
            fs.Close()
            { Pid.pid=pid } |> sendStorm
        | _ -> failwith "pidDir had no value."
    | _ -> failwith "Missing pidDir in initial handshake."
    stdin |> readmore
    0
