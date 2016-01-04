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

let readInput (log:System.IO.StreamWriter) (input:IO.TextReader) =
    let rec getInputMessage (sb:System.Text.StringBuilder) =
        //let line = input.ReadLine()
        match input.Peek() with
        //| -1 -> sb.ToString()
        | _ ->
            log.WriteLine("Reading next line...")
            log.Flush()
            match input.ReadLine() with
            | null
            | "end" -> sb.ToString()
            | s ->
                log.WriteLine(s)
                log.Flush()
                sb.AppendLine(s) |> getInputMessage
    System.Text.StringBuilder() |> getInputMessage

let writeEmit (log:System.IO.StreamWriter) =
    let emit = { EmitDoc with tuple=[|"whatever"|] }
    let os = stdout
    "About to emit tuple..." |> log.WriteLine
    //emit |> JsonConvert.SerializeObject |> os.WriteLine
    "end" |> os.WriteLine
    "Emitted tuple." |> log.WriteLine
    "Tuple emitted..." |> log.WriteLine
    log.Flush()
    os.Flush()

let writeLog msg =
    let os = stdout
    sprintf """{"command":"log", "msg":"%s"}""" msg |> os.WriteLine
    "end" |> os.WriteLine
    os.Flush()

let writeSync (log:System.IO.StreamWriter) =
    let os = stdout
    """{"command":"sync"}""" |> os.WriteLine
    "end" |> os.WriteLine
    "Sent sync" |> log.WriteLine
    log.Flush()
    os.Flush()

[<EntryPoint>]
let main argv =
    use log = System.IO.File.CreateText("/tmp/fstorm.log")
    let rec readmore (input:IO.TextReader) =
        "in readmore() " |> log.WriteLine
        log.Flush()
        match input |> readInput log with 
        | "" | null -> 
            log.WriteLine("Got no input.")
            log.Flush()
            log |> writeSync
            System.Threading.Thread.Sleep(1000)
        | line -> 
            try
                log.WriteLine("About to parse line")
                let doc = line |> Newtonsoft.Json.Linq.JObject.Parse
                line |> log.WriteLine
                log.Flush()
                log.WriteLine("ABOUT TO PROCESS INPUT")
                writeLog "about to process input."
                match doc.TryGetValue("command") with
                | true, token -> 
                    log.WriteLine("GOT COMMAND")
                    match token with 
                    | :? JValue as jval ->
                        sprintf "Matching token: %O" jval.Value |> log.WriteLine
                        match jval.Value :?> string with
                        | "ack" -> 
                            let ack = line |> SpoutAckCmd.Parse
                            printfn "%A" ack.Id
                            log |> writeSync
                        | "next" -> 
                            log |> writeEmit
                            System.Threading.Thread.Sleep(1000)
                            log |> writeSync
                        | "sync" -> 
                            log |> writeSync
                        | "fail" -> line |> SpoutFailCmd.Parse |> printfn "%A"
                        | "log" -> line |> SpoutLog.Parse |> printfn "%A"
                        | "emit" -> 
                            log |> writeEmit
                            log |> writeSync
                        | _ -> 
                            log.WriteLine("Got unknown command")
                            log.Flush()
                            failwith "Got unknown command."
                    | _ -> 
                        log.WriteLine("Got non-string command value.")
                        log.Flush()
                        failwith "Got non-string commmand value."
                | false, _ -> 
                    match doc.TryGetValue("pidDir") with 
                    | true, token -> 
                        sprintf "Got pidDir: %O" token |> log.WriteLine 
                        match token with 
                        | :? JValue as jval ->
                            let pidDir = jval.Value :?> string
                            let id = System.Diagnostics.Process.GetCurrentProcess().Id
                            sprintf "Writing pid file: %i" id |> log.WriteLine
                            use fs = System.IO.File.Create(System.IO.Path.Combine([|pidDir; id.ToString()|]))
                            fs.Close()
                            sprintf "Sending pid response: %i" id |> log.WriteLine
                            let os = stdout
                            sprintf """{"pid":%i}""" id |> os.WriteLine
                            "end" |> os.WriteLine
                            os.Flush()
                            sprintf """Sent {"pid":%i}""" id |> log.WriteLine
                            log.Flush()
                            sprintf "Done with initial spout setup: %i" id |> log.WriteLine
                        | _ ->
                            log.WriteLine("Received empty pidDir")
                            log.Flush()
                            failwith "Received empty pidDir attribute"
                    | _false, _ ->
                        log.WriteLine("Message missing command.") 
                        log.Flush()
                        failwith (sprintf "Message missing command.: %s" line)
            with
            | ex -> 
                log.WriteLine(ex)
                log.Flush()
        log.WriteLine("Will read more...")
        log.Flush()
        input |> readmore
    "Getting stdin" |> log.WriteLine
    let instrm = stdin
    log.WriteLine("Got stdin, about to call readmore")
    log.Flush()
    instrm |> readmore
    0 // return an integer exit code
