{ KeyVast - A key value store }
{ Copyright (c) 2018-2019 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/23  0.01  Initial version with Scope and Session }
{ 2019/04/19  0.02  CreateDataset record size parameters }

{$INCLUDE kvInclude.inc}

unit kvScriptContext;

interface

uses
  SysUtils,
  kvHashList,
  kvValues,
  kvAbstractSystem;



type
  { Scope }

  EkvScriptScope = class(Exception);

  AkvScriptScope = class
  public
    function  GetIdentifier(const Identifier: String): TObject; virtual; abstract;
    procedure SetIdentifier(const Identifier: String; const Value: TObject); virtual; abstract;
  end;



  { Session }

  AkvScriptSession = class
  public
    procedure ExecLock; virtual; abstract;
    procedure ExecUnlock; virtual; abstract;

    function  AllocateSystemUniqueId: UInt64; virtual; abstract;

    function  CreateDatabase(const Name: String): AkvDatabase; virtual; abstract;
    procedure DropDatabase(const Name: String); virtual; abstract;
    function  ListOfDatabases: TkvDictionaryValue; virtual; abstract;

    function  AllocateDatabaseUniqueId(const DatabaseName: String): UInt64; virtual; abstract;

    function  CreateDataset(const DatabaseName, DatasetName: String;
              const UseFolders: Boolean): AkvDataset; virtual; abstract;
    procedure DropDataset(const DatabaseName, DatasetName: String); virtual; abstract;
    function  ListOfDatasets(const DatabaseName: String): TkvDictionaryValue; virtual; abstract;

    function  AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64; virtual; abstract;

    procedure UseDatabase(const Name: String); virtual; abstract;
    procedure UseDataset(const DatabaseName, DatasetName: String); virtual; abstract;
    procedure UseNone; virtual; abstract;
    function  GetSelectedDatabaseName: String; virtual; abstract;
    function  GetSelectedDatasetName: String; virtual; abstract;

    procedure AddRecord(const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); virtual; abstract;
    procedure MakePath(const DatabaseName, DatasetName, KeyPath: String); virtual; abstract;
    function  GetRecord(const DatabaseName, DatasetName, Key: String): AkvValue; virtual; abstract;
    function  RecordExists(const DatabaseName, DatasetName, Key: String): Boolean; virtual; abstract;
    procedure DeleteRecord(const DatabaseName, DatasetName, Key: String); virtual; abstract;
    procedure SetRecord(const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); virtual; abstract;
    procedure AppendRecord(const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); virtual; abstract;

    function  ListOfKeys(const DatabaseName, DatasetName, KeyPath: String;
              const Recurse: Boolean;
              const IncludeRecordTimestamp: Boolean): AkvValue; virtual; abstract;
    function  IterateRecords(const DatabaseName, DatasetName: String;
              const Path: String;
              out Iterator: AkvDatasetIterator;
              const ARecurse: Boolean;
              const AIncludeFolders: Boolean;
              const AMinTimestamp: UInt64): Boolean; virtual; abstract;
    function  IterateFolders(const DatabaseName, DatasetName: String;
              const Path: String;
              out Iterator: AkvDatasetIterator): Boolean; virtual; abstract;
    function  IterateNextRecord(var Iterator: AkvDatasetIterator): Boolean; virtual; abstract;
    function  IteratorGetKey(const Iterator: AkvDatasetIterator): String; virtual; abstract;
    function  IteratorGetValue(const Iterator: AkvDatasetIterator): AkvValue; virtual; abstract;
    function  IteratorGetTimestamp(const Iterator: AkvDatasetIterator): Int64; virtual; abstract;

    function  ExecScript(const S: String): AkvValue; virtual; abstract;

    procedure CreateStoredProcedure(const DatabaseName, ProcedureName, Script: String); virtual; abstract;
    procedure DropStoredProcedure(const DatabaseName, ProcedureName: String); virtual; abstract;
  end;



  { Context }

  TkvScriptScopeType = (sstGlobal, sstStoredProcedure);

  TkvScriptContext = class
  protected
    FScope     : AkvScriptScope;
    FScopeType : TkvScriptScopeType;
    FSession   : AkvScriptSession;

  public
    constructor Create(const Scope: AkvScriptScope;
                const ScopeType: TkvScriptScopeType;
                const Session: AkvScriptSession);

    property  Scope: AkvScriptScope read FScope;
    property  ScopeType: TkvScriptScopeType read FScopeType;
    property  Session: AkvScriptSession read FSession;
  end;



implementation



{ TkvScriptContext }

constructor TkvScriptContext.Create(const Scope: AkvScriptScope;
            const ScopeType: TkvScriptScopeType;
            const Session: AkvScriptSession);
begin
  inherited Create;
  FScope := Scope;
  FScopeType := ScopeType;
  FSession := Session;
end;



end.

