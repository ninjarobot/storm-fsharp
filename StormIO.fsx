#r "packages/Newtonsoft.Json.7.0.1/lib/net40/Newtonsoft.Json.dll"
#r "packages/log4net.2.0.5/lib/net40-full/log4net.dll"

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
type Ack = { command:string; id:string }
let AckDoc = { Ack.command="ack"; id=null }
type Fail = { command:string; id:string }
let FailDoc = { Fail.command="fail"; id=null }

let hierarchy = log4net.LogManager.GetRepository() :?> log4net.Repository.Hierarchy.Hierarchy
let patternLayout = log4net.Layout.PatternLayout()
patternLayout.ConversionPattern <- "%date [%thread] %-5level %logger %ndc - %message%newline"
patternLayout.ActivateOptions()
let appender = log4net.Appender.FileAppender()
appender.AppendToFile <- true
appender.File <- "/tmp/fstorm.log"
appender.Layout <- patternLayout
appender.ActivateOptions()
hierarchy.Root.AddAppender(appender)
hierarchy.Root.Level <- log4net.Core.Level.Debug
hierarchy.Configured <- true

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

let generateId =
    let rng = new System.Random()
    rng.Next()

let emitToStorm (tuple:obj array) =
    let tupleId = generateId |> sprintf "%i"
    { EmitDoc with tuple=tuple; id=tupleId } |> toStorm

let logToStorm msg =
    { LogDoc with msg = msg } |> toStorm

let syncToStorm () =
    SyncDoc |> toStorm
    
let ackToStorm id = 
    { AckDoc with id=id } |> toStorm
    
let failToStorm id =
    { FailDoc with id=id } |> toStorm
