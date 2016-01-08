#r "packages/Newtonsoft.Json.7.0.1/lib/net40/Newtonsoft.Json.dll"

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

let jsonSerialize o =
    JsonConvert.SerializeObject (o, Formatting.None, JsonSerializerSettings (DefaultValueHandling=DefaultValueHandling.Ignore))

let jsonDeserialize s = 
    try 
        Some (JObject.Parse s)
    with
        | _ as ex -> None

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
