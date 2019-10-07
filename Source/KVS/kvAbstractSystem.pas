{ KeyVast - A key value store }
{ Copyright (c) 2018-2019 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2019/04/19  0.01  Initial interface. }
{ 2019/09/30  0.02  Minimal interface. }
{ 2019/09/30  0.03  Rename to kvAbstractSystem. }
{ 2019/10/05  0.04  Iterate records with Timestamp filter. }
{ 2019/10/05  0.05  Iterate records non recursive. }
{ 2019/10/05  0.06  Iterate records optionally include folders. }
{ 2019/10/05  0.07  ListOfKeys optionally return record timestamps. }
{ 2019/10/05  0.08  Dataset CopyFrom. }

{$INCLUDE kvInclude.inc}

unit kvAbstractSystem;

interface

uses
  kvValues;



type
  AkvDataset = class;

  AkvDatasetIterator = class
  public
    function  GetDataset: AkvDataset; virtual; abstract;
  end;



  AkvDataset = class
  protected
    function  GetName: String; virtual; abstract;
    function  GetUseFolders: Boolean; virtual; abstract;

  public
    property  Name: String read GetName;
    property  UseFolders: Boolean read GetUseFolders;

    function  GetTimestamp: UInt64; virtual; abstract;

    function  GetUniqueId: UInt64; virtual; abstract;
    function  AllocateUniqueId: UInt64; virtual; abstract;

    function  RecordExists(const AKey: String): Boolean; virtual; abstract;
    function  GetRecord(const AKey: String): AkvValue; virtual; abstract;
    function  GetRecordIfExists(const AKey: String): AkvValue; virtual; abstract;
    function  FolderExists(const AKey: String): Boolean; virtual; abstract;
    procedure MakePath(const AKeyPath: String); virtual; abstract;
    procedure AddRecord(const AKey: String; const AValue: AkvValue); virtual; abstract;
    procedure SetRecord(const AKey: String; const AValue: AkvValue); virtual; abstract;
    procedure SetOrAddRecord(const AKey: String; const AValue: AkvValue); virtual; abstract;
    procedure AppendRecord(const AKey: String; const AValue: AkvValue); virtual; abstract;
    procedure DeleteRecord(const AKey: String); virtual; abstract;
    procedure DeleteFolderRecords(const APath: String); virtual; abstract;

    function  ListOfKeys(
              const AKeyPath: String;
              const ARecurse: Boolean;
              const AIncludeRecordTimestamp: Boolean = False): TkvDictionaryValue; virtual; abstract;
    function  IterateRecords(
              const APath: String;
              out AIterator: AkvDatasetIterator;
              const ARecurse: Boolean = True;
              const AIncludeFolders: Boolean = False;
              const AMinTimestamp: UInt64 = 0): Boolean; virtual; abstract;
    function  IterateFolders(const APath: String; out AIterator: AkvDatasetIterator): Boolean; virtual; abstract;
    function  IterateNextRecord(var AIterator: AkvDatasetIterator): Boolean; virtual; abstract;
    function  IteratorHasRecord(const AIterator: AkvDatasetIterator): Boolean; virtual; abstract;
    function  IteratorIsFolder(const AIterator: AkvDatasetIterator): Boolean; virtual; abstract;
    function  IteratorGetKey(const AIterator: AkvDatasetIterator): String; virtual; abstract;
    function  IteratorGetValue(const AIterator: AkvDatasetIterator): AkvValue; virtual; abstract;
    function  IteratorGetTimestamp(const AIterator: AkvDatasetIterator): UInt64; virtual; abstract;

    procedure CopyFrom(const ADataset: AkvDataset); virtual; abstract;
  end;



  AkvDatabase = class;

  AkvDatasetListIterator = class
  public
    function  GetDatabase: AkvDatabase; virtual; abstract;
    function  GetName: String; virtual; abstract;
    function  GetDataset: AkvDataset; virtual; abstract;
  end;



  AkvDatabase = class
  protected
    function  GetName: String; virtual; abstract;

  public
    function  AllocateUniqueId: UInt64; virtual; abstract;

    property  Name: String read GetName;

    function  DatasetExists(const AName: String): Boolean; virtual; abstract;
    function  RequireDatasetByName(const AName: String): AkvDataset; virtual; abstract;
    function  AddDataset(const AName: String; const AUseFolders: Boolean): AkvDataset; virtual; abstract;
    procedure RemoveDataset(const AName: String); virtual; abstract;
    function  GetDatasetCount: Integer; virtual; abstract;
    function  IterateFirstDataset(var AIterator: AkvDatasetListIterator): Boolean; virtual; abstract;
    function  IterateNextDataset(var AIterator: AkvDatasetListIterator): Boolean; virtual; abstract;
  end;



  AkvDatabaseListIterator = class
  public
    function  GetName: String; virtual; abstract;
    function  GetDatabase: AkvDatabase; virtual; abstract;
  end;



  AkvSystem = class
  protected
    function  GetUserDataStr: String; virtual; abstract;
    procedure SetUserDataStr(const A: String); virtual; abstract;

  public
    property  UserDataStr: String read GetUserDataStr write SetUserDataStr;

    function  AllocateUniqueId: UInt64; virtual; abstract;

    function  DatabaseExists(const AName: String): Boolean; virtual; abstract;
    function  RequireDatabaseByName(const AName: String): AkvDatabase; virtual; abstract;
    function  CreateDatabase(const AName: String): AkvDatabase; virtual; abstract;
    procedure DropDatabase(const AName: String); virtual; abstract;
    function  GetDatabaseCount: Integer; virtual; abstract;
    function  IterateFirstDatabase(var AIterator: AkvDatabaseListIterator): Boolean; virtual; abstract;
    function  IterateNextDatabase(var AIterator: AkvDatabaseListIterator): Boolean; virtual; abstract;
  end;



implementation




end.

