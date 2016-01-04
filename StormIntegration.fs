module StormIntegration

open System
open FSharp.Data
open FSharp.Data.JsonExtensions
open Newtonsoft.Json
open Newtonsoft.Json.Linq

type SetupInfo = JsonProvider<"Protocol/SetupInfo.json">
type Pid = JsonProvider<"Protocol/Pid.json">
type SpoutNextCmd = JsonProvider<"Protocol/SpoutNextCmd.json">
type SpoutAckCmd = JsonProvider<"Protocol/SpoutAckCmd.json">
type SpoutFailCmd = JsonProvider<"Protocol/SpoutFailCmd.json">
type SpoutEmit = JsonProvider<"Protocol/SpoutEmit.json">
type SpoutLog = JsonProvider<"Protocol/SpoutLog.json">
type SpoutSyncCmd = JsonProvider<"Protocol/SpoutSyncCmd.json">

type Emit = { command:string; id:string; stream:int; task:int; tuple:obj array }
let EmitDoc = { Emit.command="emit"; id="0"; stream=0; task=0; tuple=[||] }

log4net.Config.XmlConfigurator.Configure() |> ignore

let log = log4net.LogManager.GetLogger("StormIntegration")

let readInput (input:IO.TextReader) =
    let rec getInputMessage (sb:System.Text.StringBuilder) =
        match input.ReadLine() with
        | null
        | "end" -> sb.ToString()
        | s ->
            sb.AppendLine(s) |> getInputMessage
    System.Text.StringBuilder() |> getInputMessage

let writeEmit () =
    log.InfoFormat("About to emit()")
    let os = stdout
    """{"command":"emit", "tuple":["whatever"]}""" |> os.WriteLine
    "end" |> os.WriteLine
    os.Flush()
    log.InfoFormat("emit complete")

let writeLog msg =
    let os = stdout
    sprintf """{"command":"log", "msg":"%s"}""" msg |> os.WriteLine
    "end" |> os.WriteLine
    os.Flush()

let writeSync () =
    log.InfoFormat("About to sync()")
    let os = stdout
    """{"command":"sync"}""" |> os.WriteLine
    "end" |> os.WriteLine
    os.Flush()
    log.InfoFormat("sync complete")

let rec readmore (input:IO.TextReader) =
    let line = input |> readInput 
    log.InfoFormat("Got line: {0}", line)
    try 
        let doc = line |> Newtonsoft.Json.Linq.JObject.Parse
        writeLog "about to process input."
        match doc.TryGetValue("command") with
        | true, token -> 
            match token with 
            | :? JValue as jval ->
                match jval.Value :?> string with
                | "next" -> 
                    log.InfoFormat("Got next command, write some tuples!!!!!")
                    writeEmit ()
                    writeEmit ()
                    writeEmit ()
                    writeEmit ()
                    writeEmit ()
                    writeEmit ()
                | _ -> 
                    failwith "Got unknown command."
            | _ -> 
                failwith "Got non-string commmand value."
        | false, _ -> 
            failwith (sprintf "Message missing command.: %s" line)
        System.Threading.Thread.Sleep(1)
        writeSync ()
    with
    | _ as ex -> log.WarnFormat("Line {0} resulted in exception: {1}", line, ex)
    input |> readmore

[<EntryPoint>]
let main argv =
    log.InfoFormat("Logging initialized.")
    let line = stdin |> readInput
    let config = line |> Newtonsoft.Json.Linq.JObject.Parse
    match config.TryGetValue("pidDir") with 
    | true, token ->
        match token with
        | :? JValue as jval ->
            let pidDir = jval.Value :?> string
            let id = System.Diagnostics.Process.GetCurrentProcess().Id
            use fs = System.IO.File.Create(System.IO.Path.Combine(pidDir, id.ToString()))
            fs.Close()
            let os = stdout
            sprintf """{"pid":%i}""" id |> os.WriteLine
            "end" |> os.WriteLine
            os.Flush()
            log.InfoFormat("Pid sent")
        | _ -> failwith "pidDir had no value."
    | _ -> failwith "Missing pidDir in initial handshake."
    stdin |> readmore
    0 // return an integer exit code
