{ KeyVast - A key value store }
{ Copyright (c) 2018-2019 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/09  0.01  Initial version (int, str, float, list, dict) }
{ 2018/02/14  0.02  List and dictionary accessor functions }
{ 2018/02/16  0.03  Boolean }
{ 2018/02/17  0.04  Binary operators }
{ 2018/02/18  0.05  Null value, DateTime }
{ 2018/02/26  0.06  VarWord32 encoding }
{ 2018/03/01  0.07  Binary value }
{ 2018/03/03  0.08  Unordered set value }
{ 2018/03/04  0.09  Optimised dictionary implementation }
{ 2018/03/05  0.10  Value GetDataBuf function }
{ 2018/03/11  0.11  Decimal128 value }
{ 2018/03/12  0.12  Value operation optimisations }
{ 2018/09/27  0.13  AddOrSetValue }
{ 2019/04/19  0.14  CreateInstance class function }
{ 2019/09/13  0.15  Folder value }

{$INCLUDE kvInclude.inc}

unit kvValues;

interface

uses
  SysUtils,
  flcDecimal,
  kvHashList;



const
  KV_Value_TypeId_Integer    = $01;
  KV_Value_TypeId_String     = $02;
  KV_Value_TypeId_Float      = $03;
  KV_Value_TypeId_Boolean    = $04;
  KV_Value_TypeId_DateTime   = $05;
  KV_Value_TypeId_Binary     = $06;
  KV_Value_TypeId_Decimal128 = $07;
  KV_Value_TypeId_Null       = $10;
  KV_Value_TypeId_List       = $20;
  KV_Value_TypeId_Dictionary = $21;
  KV_Value_TypeId_Set        = $22;
  KV_Value_TypeId_Folder     = $30;
  KV_Value_TypeId_Other      = $FF;



type
  kvByteArray = array of Byte;

  EkvValue = class(Exception);

  AkvValue = class
  protected
    function  GetAsString: String; virtual; abstract;
    function  GetAsScript: String; virtual;
    function  GetAsBoolean: Boolean; virtual;
    function  GetAsFloat: Double; virtual;
    function  GetAsInteger: Int64; virtual;
    function  GetAsDateTime: TDateTime; virtual;
    function  GetAsBinary: kvByteArray; virtual;
    function  GetAsDecimal128: SDecimal128; virtual;
    procedure SetAsString(const A: String); virtual;
    procedure SetAsBoolean(const A: Boolean); virtual;
    procedure SetAsFloat(const A: Double); virtual;
    procedure SetAsInteger(const A: Int64); virtual;
    procedure SetAsDateTime(const A: TDateTime); virtual;
    procedure SetAsBinary(const A: kvByteArray); virtual;
    procedure SetAsDecimal128(const A: SDecimal128); virtual;
    function  GetTypeId: Byte; virtual;
    function  GetSerialSize: Integer; virtual;
  public
    class function CreateInstance: AkvValue; virtual; abstract;
    function  Duplicate: AkvValue; virtual; abstract;
    property  AsString: String read GetAsString write SetAsString;
    property  AsScript: String read GetAsScript;
    property  AsBoolean: Boolean read GetAsBoolean write SetAsBoolean;
    property  AsFloat: Double read GetAsFloat write SetAsFloat;
    property  AsInteger: Int64 read GetAsInteger write SetAsInteger;
    property  AsDateTime: TDateTime read GetAsDateTime write SetAsDateTime;
    property  AsBinary: kvByteArray read GetAsBinary write SetAsBinary;
    property  AsDecimal128: SDecimal128 read GetAsDecimal128 write SetAsDecimal128;
    procedure Negate; virtual;
    property  TypeId: Byte read GetTypeId;
    property  SerialSize: Integer read GetSerialSize;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; virtual;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; virtual;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); virtual;
  end;
  TkvValueClass = class of AkvValue;
  TkvValueArray = array of AkvValue;

  TkvIntegerValue = class(AkvValue)
  private
    FValue : Int64;
  protected
    function  GetAsString: String; override;
    function  GetAsBoolean: Boolean; override;
    function  GetAsFloat: Double; override;
    function  GetAsInteger: Int64; override;
    function  GetAsDecimal128: SDecimal128; override;
    procedure SetAsString(const A: String); override;
    procedure SetAsBoolean(const A: Boolean); override;
    procedure SetAsInteger(const A: Int64); override;
    procedure SetAsDecimal128(const A: SDecimal128); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: Int64); overload;
    property  Value: Int64 read FValue write FValue;
    function  Duplicate: AkvValue; override;
    procedure Negate; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvStringValue = class(AkvValue)
  private
    FValue : String;
  protected
    function  GetAsString: String; override;
    function  GetAsScript: String; override;
    function  GetAsBoolean: Boolean; override;
    function  GetAsFloat: Double; override;
    function  GetAsInteger: Int64; override;
    function  GetAsDateTime: TDateTime; override;
    function  GetAsBinary: kvByteArray; override;
    function  GetAsDecimal128: SDecimal128; override;
    procedure SetAsString(const A: String); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: String); overload;
    property  Value: String read FValue write FValue;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvFloatValue = class(AkvValue)
  private
    FValue : Double;
  protected
    function  GetAsString: String; override;
    function  GetAsFloat: Double; override;
    function  GetAsDecimal128: SDecimal128; override;
    procedure SetAsFloat(const A: Double); override;
    procedure SetAsInteger(const A: Int64); override;
    procedure SetAsDecimal128(const A: SDecimal128); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: Double); overload;
    property  Value: Double read FValue write FValue;
    function  Duplicate: AkvValue; override;
    procedure Negate; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvBooleanValue = class(AkvValue)
  private
    FValue : Boolean;
  protected
    function  GetAsString: String; override;
    function  GetAsBoolean: Boolean; override;
    procedure SetAsString(const A: String); override;
    procedure SetAsBoolean(const A: Boolean); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: Boolean); overload;
    property  Value: Boolean read FValue write FValue;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvDateTimeValue = class(AkvValue)
  private
    FValue : TDateTime;
  protected
    function  GetAsString: String; override;
    function  GetAsScript: String; override;
    function  GetAsDateTime: TDateTime; override;
    procedure SetAsDateTime(const A: TDateTime); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: TDateTime); overload;
    property  Value: TDateTime read FValue write FValue;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvBinaryValue = class(AkvValue)
  private
    FValue : kvByteArray;
  protected
    function  GetAsString: String; override;
    function  GetAsScript: String; override;
    function  GetAsBinary: kvByteArray; override;
    procedure SetAsString(const A: String); override;
    procedure SetAsBinary(const A: kvByteArray); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: kvByteArray); overload;
    constructor Create(const Value: Byte); overload;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvDecimal128Value = class(AkvValue)
  private
    FValue : SDecimal128;
  protected
    function  GetAsString: String; override;
    function  GetAsFloat: Double; override;
    function  GetAsInteger: Int64; override;
    function  GetAsDecimal128: SDecimal128; override;
    procedure SetAsString(const A: String); override;
    procedure SetAsInteger(const A: Int64); override;
    procedure SetAsFloat(const A: Double); override;
    procedure SetAsDecimal128(const A: SDecimal128); override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: SDecimal128); overload;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
  end;

  TkvNullValue = class(AkvValue)
  protected
    function  GetAsString: String; override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure GetDataBuf(out Buf: Pointer; out BufSize: Integer); override;
  end;

  TkvListValue = class(AkvValue)
  private
    FValue : array of AkvValue;
  protected
    function  GetAsString: String; override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    destructor Destroy; override;
    function  GetCount: Integer;
    function  Duplicate: AkvValue; override;
    class function EncodeEntry(var Buf; const BufSize: Integer; const Value: AkvValue): Integer;
    function  EncodeEntries(var Buf; const BufSize: Integer): Integer;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure Add(const Value: AkvValue);
    procedure AddList(const List: TkvListValue);
    function  GetValue(const Index: Integer): AkvValue;
    procedure SetValue(const Index: Integer; const Value: AkvValue);
    procedure DeleteValue(const Index: Integer);
    procedure InsertValue(const Index: Integer; const Value: AkvValue);
    procedure AppendValue(const Value: AkvValue);
    function  HasValue(const Value: AkvValue): Boolean;
  end;

  TkvDictionaryValueIterator = record
    Itr : TkvStringHashListIterator;
    Itm : PkvStringHashListItem;
  end;

  TkvDictionaryValue = class(AkvValue)
  private
    FValue : TkvStringHashList;
  protected
    function  GetAsString: String; override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create;
    destructor Destroy; override;
    procedure Clear;
    function  Duplicate: AkvValue; override;
    class function EncodeEntry(var Buf; const BufSize: Integer;
                   const Key: String; const Value: AkvValue): Integer;
    function  EncodeEntries(var Buf; const BufSize: Integer): Integer;
    function  EncodedEntriesSize: Integer;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure Add(const Key: String; const Value: AkvValue);
    procedure AddItems(const Items: TkvDictionaryValue);
    procedure AddString(const Key: String; const Value: String);
    procedure AddBoolean(const Key: String; const Value: Boolean);
    procedure AddFloat(const Key: String; const Value: Double);
    procedure AddInteger(const Key: String; const Value: Int64);
    procedure AddDateTime(const Key: String; const Value: TDateTime);
    procedure AddBinary(const Key: String; const Value: kvByteArray);
    procedure AddNull(const Key: String);
    function  Exists(const Key: String): Boolean;
    function  IsValueNull(const Key: String): Boolean;
    function  GetValue(const Key: String): AkvValue;
    function  GetValueAsString(const Key: String): String;
    function  GetValueAsBoolean(const Key: String): Boolean;
    function  GetValueAsFloat(const Key: String): Double;
    function  GetValueAsInteger(const Key: String): Int64;
    function  GetValueAsDateTime(const Key: String): TDateTime;
    function  GetValueAsBinary(const Key: String): kvByteArray;
    function  GetValueIfExists(const Key: String): AkvValue;
    function  GetValueAsStringDef(const Key: String; const ADefault: String = ''): String;
    function  GetValueAsBooleanDef(const Key: String; const ADefault: Boolean = False): Boolean;
    function  GetValueAsFloatDef(const Key: String; const ADefault: Double = 0.0): Double;
    function  GetValueAsIntegerDef(const Key: String; const ADefault: Int64 = 0): Int64;
    function  GetValueAsDateTimeDef(const Key: String; const ADefault: TDateTime = 0.0): TDateTime;
    function  GetValueAsBinaryDef(const Key: String; const ADefault: kvByteArray = nil): kvByteArray;
    procedure SetValueAsString(const Key: String; const Value: String);
    procedure SetValueAsBoolean(const Key: String; const Value: Boolean);
    procedure SetValueAsInteger(const Key: String; const Value: Int64);
    procedure SetValueAsDateTime(const Key: String; const Value: TDateTime);
    procedure SetValueAsBinary(const Key: String; const Value: kvByteArray);
    procedure SetValue(const Key: String; const Value: AkvValue);
    procedure SetValueString(const Key: String; const Value: String);
    procedure SetValueBoolean(const Key: String; const Value: Boolean);
    procedure SetValueInteger(const Key: String; const Value: Int64);
    procedure SetValueDateTime(const Key: String; const Value: TDateTime);
    procedure SetValueBinary(const Key: String; const Value: kvByteArray);
    procedure SetValueNull(const Key: String);
    procedure AddOrSetValue(const Key: String; const Value: AkvValue);
    procedure AddOrSetValueString(const Key: String; const Value: String);
    procedure AddOrSetValueBoolean(const Key: String; const Value: Boolean);
    procedure AddOrSetValueInteger(const Key: String; const Value: Int64);
    procedure AddOrSetValueDateTime(const Key: String; const Value: TDateTime);
    procedure AddOrSetValueBinary(const Key: String; const Value: kvByteArray);
    procedure DeleteKey(const Key: String);
    function  ReleaseKey(const Key: String): TObject;
    function  IterateFirst(out Iterator: TkvDictionaryValueIterator): Boolean;
    function  IterateNext(out Iterator: TkvDictionaryValueIterator): Boolean;
    procedure IteratorGetKeyValue(const Iterator: TkvDictionaryValueIterator;
              out Key: String; out Value: AkvValue);
    function  GetCount: Integer;
  end;

  TkvSetValue = class(AkvValue)
  private
    FValue : TkvStringHashList;
  protected
    function  GetAsString: String; override;
    function  GetTypeId: Byte; override;
    function  GetSerialSize: Integer; override;
  public
    class function CreateInstance: AkvValue; override;
    constructor Create; overload;
    constructor Create(const Value: TkvStringHashList); overload;
    destructor Destroy; override;
    function  Duplicate: AkvValue; override;
    function  GetSerialBuf(var Buf; const BufSize: Integer): Integer; override;
    function  PutSerialBuf(const Buf; const BufSize: Integer): Integer; override;
    procedure Clear;
    procedure Add(const Key: String);
    procedure AddSet(const Value: TkvSetValue);
    function  Exists(const Key: String): Boolean;
    procedure DeleteKey(const Key: String);
    procedure DeleteSet(const Value: TkvSetValue);
  end;

  TkvFolderValue = class(TkvDictionaryValue)
  protected
    function GetTypeId: Byte; override;
  public
    class function CreateInstance: AkvValue; override;
  end;



{ Factory function }

function kvGetValueClassFromTypeId(const TypeId: Integer): TkvValueClass;
function kvCreateValueFromTypeId(const TypeId: Integer): AkvValue;



{ Value operators }

function ValueOpPlus(const A, B: AkvValue): AkvValue;
function ValueOpMinus(const A, B: AkvValue): AkvValue;
function ValueOpMultiply(const A, B: AkvValue): AkvValue;
function ValueOpDivide(const A, B: AkvValue): AkvValue;

function ValueOpOR(const A, B: AkvValue): AkvValue;
function ValueOpXOR(const A, B: AkvValue): AkvValue;
function ValueOpAND(const A, B: AkvValue): AkvValue;
function ValueOpNOT(const A: AkvValue): AkvValue;

function ValueOpCompare(const A, B: AkvValue): Integer;

function ValueOpIn(const A, B: AkvValue): Boolean;

function ValueOpAppend(const A, B: AkvValue): AkvValue;



{ VarWord32 functions }

type
  Int32 = FixedInt;
  PInt32 = ^Int32;
  Word32 = FixedUInt;
  PWord32 = ^Word32;

function kvVarWord32EncodedSize(const A: Word32): Integer;
function kvVarWord32EncodeBuf(const A: Word32; var Buf; const BufSize: Integer): Integer;
function kvVarWord32DecodeBuf(const Buf; const BufSize: Integer; out A: Word32): Integer;



{ Timestamp encoding }

const
  KV_Timestamp_Min = 0;
  KV_Timestamp_Max = $00FFFFFFFFFFFFFF;

function kvDateTimeToTimestamp(const D: TDateTime): Int64;
function kvTimestampNow: Int64;



implementation

uses
  StrUtils,

  System.DateUtils;



const
  SInvalidBufferSize = 'Invalid buffer size';



{ VarWord32 functions }

function kvVarWord32EncodedSize(const A: Word32): Integer;
begin
  if A <= $7F then
    Result := 1
  else
    Result := 4;
end;

function kvVarWord32EncodeBuf(const A: Word32; var Buf; const BufSize: Integer): Integer;
var
  E : Word32;
begin
  if A <= $7F then
    begin
      if BufSize < 1 then
        raise EkvValue.Create(SInvalidBufferSize);
      PByte(@Buf)^ := Byte(A);
      Result := 1;
    end
  else
  if A >= $80000000 then
    raise EkvValue.Create('VarInt value overflow')
  else
    begin
      if BufSize < 4 then
        raise EkvValue.Create(SInvalidBufferSize);
      E := (A and $7F) or $80 or ((A shl 1) and $FFFFFF00);
      PWord32(@Buf)^ := E;
      Result := 4;
    end;
end;

function kvVarWord32DecodeBuf(const Buf; const BufSize: Integer; out A: Word32): Integer;
var
  B : Byte;
  E : Word32;
begin
  if BufSize < 1 then
    raise EkvValue.Create(SInvalidBufferSize);
  B := PByte(@Buf)^;
  if B and $80 = 0 then
    begin
      A := B;
      Result := 1;
    end
  else
    begin
      if BufSize < 4 then
        raise EkvValue.Create(SInvalidBufferSize);
      E := PWord32(@Buf)^;
      A := (E and $7F) or ((E shr 1) and $FFFFFF80);
      Result := 4;
    end;
end;



{ Factory function }

function kvGetValueClassFromTypeId(const TypeId: Integer): TkvValueClass;
begin
  case TypeId of
    KV_Value_TypeId_Integer    : Result := TkvIntegerValue;
    KV_Value_TypeId_String     : Result := TkvStringValue;
    KV_Value_TypeId_Float      : Result := TkvFloatValue;
    KV_Value_TypeId_Boolean    : Result := TkvBooleanValue;
    KV_Value_TypeId_DateTime   : Result := TkvDateTimeValue;
    KV_Value_TypeId_Binary     : Result := TkvBinaryValue;
    KV_Value_TypeId_Null       : Result := TkvNullValue;
    KV_Value_TypeId_Decimal128 : Result := TkvDecimal128Value;
    KV_Value_TypeId_List       : Result := TkvListValue;
    KV_Value_TypeId_Dictionary : Result := TkvDictionaryValue;
    KV_Value_TypeId_Set        : Result := TkvSetValue;
    KV_Value_TypeId_Folder     : Result := TkvFolderValue;
  else
    raise EkvValue.Create('Invalid value type id');
  end;
end;

function kvCreateValueFromTypeId(const TypeId: Integer): AkvValue;
begin
  case TypeId of
    KV_Value_TypeId_Integer    : Result := TkvIntegerValue.Create;
    KV_Value_TypeId_String     : Result := TkvStringValue.Create;
    KV_Value_TypeId_Float      : Result := TkvFloatValue.Create;
    KV_Value_TypeId_Boolean    : Result := TkvBooleanValue.Create;
    KV_Value_TypeId_DateTime   : Result := TkvDateTimeValue.Create;
    KV_Value_TypeId_Binary     : Result := TkvBinaryValue.Create;
    KV_Value_TypeId_Null       : Result := TkvNullValue.Create;
    KV_Value_TypeId_Decimal128 : Result := TkvDecimal128Value.Create;
    KV_Value_TypeId_List       : Result := TkvListValue.Create;
    KV_Value_TypeId_Dictionary : Result := TkvDictionaryValue.Create;
    KV_Value_TypeId_Set        : Result := TkvSetValue.Create;
    KV_Value_TypeId_Folder     : Result := TkvFolderValue.Create;
  else
    raise EkvValue.Create('Invalid value type id');
  end;
end;



{ AkvValue }

function AkvValue.GetAsScript: String;
begin
  Result := GetAsString;
end;

function AkvValue.GetAsBoolean: Boolean;
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot convert to a boolean value', [ClassName]);
end;

function AkvValue.GetAsFloat: Double;
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot convert to a float value', [ClassName]);
end;

function AkvValue.GetAsInteger: Int64;
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot convert to a integer value', [ClassName]);
end;

function AkvValue.GetAsDateTime: TDateTime;
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot convert to a datetime value', [ClassName]);
end;

function AkvValue.GetAsBinary: kvByteArray;
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot convert to a binary value', [ClassName]);
end;

function AkvValue.GetAsDecimal128: SDecimal128;
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot convert to a decimal value', [ClassName]);
end;

procedure AkvValue.SetAsString(const A: String);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a string value', [ClassName]);
end;

procedure AkvValue.SetAsBoolean(const A: Boolean);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a boolean value', [ClassName]);
end;

procedure AkvValue.SetAsFloat(const A: Double);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a float value', [ClassName]);
end;

procedure AkvValue.SetAsInteger(const A: Int64);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a integer value', [ClassName]);
end;

procedure AkvValue.SetAsDateTime(const A: TDateTime);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a datetime value', [ClassName]);
end;

procedure AkvValue.SetAsBinary(const A: kvByteArray);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a binary value', [ClassName]);
end;

procedure AkvValue.SetAsDecimal128(const A: SDecimal128);
begin
  raise EkvValue.CreateFmt('Type conversion error: %s cannot set from a decimal value', [ClassName]);
end;

procedure AkvValue.Negate;
begin
  raise EkvValue.CreateFmt('Invalid operation on type: %s cannot negate', [ClassName]);
end;

function AkvValue.GetTypeId: Byte;
begin
  raise EkvValue.CreateFmt('Type serialisation error: %s has no type id', [ClassName]);
end;

function AkvValue.GetSerialSize: Integer;
begin
  raise EkvValue.CreateFmt('Type serialisation error: %s cannot serialise', [ClassName]);
end;

function AkvValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  raise EkvValue.CreateFmt('Type serialisation error: %s cannot serialise', [ClassName]);
end;

function AkvValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  raise EkvValue.CreateFmt('Type serialisation error: %s cannot serialise', [ClassName]);
end;

procedure AkvValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
begin
  raise EkvValue.CreateFmt('Type serialisation error: %s cannot get data buffer', [ClassName]);
end;



{ TkvIntegerValue }

class function TkvIntegerValue.CreateInstance: AkvValue;
begin
  Result := TkvIntegerValue.Create;
end;

constructor TkvIntegerValue.Create;
begin
  inherited Create;
end;

constructor TkvIntegerValue.Create(const Value: Int64);
begin
  inherited Create;
  FValue := Value;
end;

function TkvIntegerValue.GetAsString: String;
begin
  Result := IntToStr(FValue);
end;

function TkvIntegerValue.GetAsBoolean: Boolean;
begin
  Result := FValue <> 0;
end;

function TkvIntegerValue.GetAsFloat: Double;
begin
  Result := FValue;
end;

function TkvIntegerValue.GetAsInteger: Int64;
begin
  Result := FValue;
end;

function TkvIntegerValue.GetAsDecimal128: SDecimal128;
begin
  SDecimal128InitInt64(Result, FValue);
end;

procedure TkvIntegerValue.SetAsString(const A: String);
begin
  if not TryStrToInt64(A, FValue) then
    raise EkvValue.Create('Type conversion error: Invalid integer string value');
end;

procedure TkvIntegerValue.SetAsBoolean(const A: Boolean);
begin
  FValue := Ord(A);
end;

procedure TkvIntegerValue.SetAsInteger(const A: Int64);
begin
  FValue := A;
end;

procedure TkvIntegerValue.SetAsDecimal128(const A: SDecimal128);
begin
  FValue := SDecimal128ToInt64(A);
end;

function TkvIntegerValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Integer;
end;

procedure TkvIntegerValue.Negate;
begin
  FValue := -FValue;
end;

function TkvIntegerValue.Duplicate: AkvValue;
begin
  Result := TkvIntegerValue.Create(FValue);
end;

function TkvIntegerValue.GetSerialSize: Integer;
begin
  Result := SizeOf(Int64);
end;

function TkvIntegerValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(Int64) then
    raise EkvValue.Create(SInvalidBufferSize);
  PInt64(@Buf)^ := FValue;
  Result := SizeOf(Int64);
end;

function TkvIntegerValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(Int64) then
    raise EkvValue.Create(SInvalidBufferSize);
  FValue := PInt64(@Buf)^;
  Result := SizeOf(Int64);
end;

procedure TkvIntegerValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
begin
  Buf := @FValue;
  BufSize := SizeOf(Int64);
end;



{ TkvStringValue }

class function TkvStringValue.CreateInstance: AkvValue;
begin
  Result := TkvStringValue.Create;
end;

constructor TkvStringValue.Create;
begin
  inherited Create;
end;

constructor TkvStringValue.Create(const Value: String);
begin
  inherited Create;
  FValue := Value;
end;

function TkvStringValue.Duplicate: AkvValue;
begin
  Result := TkvStringValue.Create(FValue);
end;

function TkvStringValue.GetAsString: String;
begin
  Result := FValue;
end;

procedure TkvStringValue.SetAsString(const A: String);
begin
  FValue := A;
end;

function TkvStringValue.GetAsScript: String;
begin
  Result := '"' + ReplaceStr(FValue, '"', '""') + '"';
end;

function TkvStringValue.GetAsBoolean: Boolean;
begin
  if SameText(FValue, 'true') then
    Result := True
  else
  if SameText(FValue, 'false') then
    Result := False
  else
    raise EkvValue.Create('Type conversion error: Not a boolean value');
end;

function TkvStringValue.GetAsFloat: Double;
begin
  if not TryStrToFloat(FValue, Result) then
    raise EkvValue.Create('Type conversion error: Not a float value');
end;

function TkvStringValue.GetAsInteger: Int64;
begin
  if not TryStrToInt64(FValue, Result) then
    raise EkvValue.Create('Type conversion error: Not an integer value');
end;

function TkvStringValue.GetAsDateTime: TDateTime;
begin
  if not TryStrToDateTime(FValue, Result) then
    raise EkvValue.Create('Type conversion error: Not a date/time value');
end;

function TkvStringValue.GetAsDecimal128: SDecimal128;
begin
  if TryStrToSDecimal128(FValue, Result) <> dceNoError then
    raise EkvValue.Create('Type conversion error: Not a decimal value');
end;

function TkvStringValue.GetAsBinary: kvByteArray;
var
  R : kvByteArray;
  I, L : Integer;
  C : WideChar;
begin
  L := Length(FValue);
  SetLength(R, L);
  for I := 0 to L - 1 do
    begin
      C := FValue.Chars[I];
      if Ord(C) > $FF then
        raise EkvValue.Create('Type conversion error: Not a valid binary string');
      R[I] := Byte(Ord(C));
    end;
  Result := R;
end;

function TkvStringValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_String;
end;

function TkvStringValue.GetSerialSize: Integer;
var
  L : Integer;
begin
  L := Length(FValue);
  Result := kvVarWord32EncodedSize(L) + L * SizeOf(Char);
end;

function TkvStringValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
var
  L : Int32;
  N : Integer;
  M : Integer;
  P : PByte;
begin
  L := Length(FValue);
  N := kvVarWord32EncodedSize(L);
  M := N + L * SizeOf(Char);
  if BufSize < M then
    raise EkvValue.Create(SInvalidBufferSize);
  P := @Buf;
  kvVarWord32EncodeBuf(L, P^, N);
  Inc(P, N);
  if L > 0 then
    Move(PChar(FValue)^, P^, L * SizeOf(Char));
  Result := M;
end;

function TkvStringValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  N : Integer;
  Len : Word32;
  M : Int32;
  S : String;
begin
  P := @Buf;
  L := BufSize;

  N := kvVarWord32DecodeBuf(P^, L, Len);
  Dec(L, N);
  Inc(P, N);
  if Int32(Len) < 0 then
    raise EkvValue.Create('Invalid buffer: string length');

  M := Int32(Len) * SizeOf(Char);
  if L < M then
    raise EkvValue.Create(SInvalidBufferSize);
  Dec(L, M);
  SetLength(S, Len);
  if Len > 0 then
    Move(P^, PChar(S)^, M);
  FValue := S;

  Result := BufSize - L;
end;

procedure TkvStringValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
var
  L : Integer;
begin
  L := Length(FValue);
  if L = 0 then
    begin
      Buf := nil;
      BufSize := 0;
    end
  else
    begin
      Buf := Pointer(FValue);
      BufSize := L * SizeOf(Char);
    end;
end;



{ TkvFloatValue }

class function TkvFloatValue.CreateInstance: AkvValue;
begin
  Result := TkvFloatValue.Create;
end;

constructor TkvFloatValue.Create;
begin
  inherited Create;
end;

constructor TkvFloatValue.Create(const Value: Double);
begin
  inherited Create;
  FValue := Value;
end;

function TkvFloatValue.Duplicate: AkvValue;
begin
  Result := TkvFloatValue.Create(FValue);
end;

function TkvFloatValue.GetAsDecimal128: SDecimal128;
begin
  SDecimal128InitFloat(Result, FValue);
end;

function TkvFloatValue.GetAsFloat: Double;
begin
  Result := FValue;
end;

function TkvFloatValue.GetAsString: String;
begin
  Result := FloatToStr(FValue);
end;

procedure TkvFloatValue.SetAsDecimal128(const A: SDecimal128);
begin
  FValue := SDecimal128ToFloat(A);
end;

procedure TkvFloatValue.SetAsFloat(const A: Double);
begin
  FValue := A;
end;

procedure TkvFloatValue.SetAsInteger(const A: Int64);
begin
  FValue := A;
end;

function TkvFloatValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Float;
end;

procedure TkvFloatValue.Negate;
begin
  FValue := -FValue;
end;

function TkvFloatValue.GetSerialSize: Integer;
begin
  Result := SizeOf(Double);
end;

function TkvFloatValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(Double) then
    raise EkvValue.Create(SInvalidBufferSize);
  PDouble(@Buf)^ := FValue;
  Result := SizeOf(Double);
end;

function TkvFloatValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(Double) then
    raise EkvValue.Create(SInvalidBufferSize);
  FValue := PDouble(@Buf)^;
  Result := SizeOf(Double);
end;

procedure TkvFloatValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
begin
  Buf := @FValue;
  BufSize := SizeOf(Double);
end;



{ TkvBooleanValue }

class function TkvBooleanValue.CreateInstance: AkvValue;
begin
  Result := TkvBooleanValue.Create;
end;

constructor TkvBooleanValue.Create;
begin
  inherited Create;
end;

constructor TkvBooleanValue.Create(const Value: Boolean);
begin
  inherited Create;
  FValue := Value;
end;

function TkvBooleanValue.Duplicate: AkvValue;
begin
  Result := TkvBooleanValue.Create(FValue);
end;

function TkvBooleanValue.GetAsString: String;
begin
  if GetAsBoolean then
    Result := 'true'
  else
    Result := 'false';
end;

function TkvBooleanValue.GetAsBoolean: Boolean;
begin
  Result := FValue;
end;

procedure TkvBooleanValue.SetAsString(const A: String);
begin
  if A = 'true' then
    FValue := True
  else
  if A = 'false' then
    FValue := False
  else
    raise EkvValue.Create('Type conversion error: Not a boolean string value');
end;

procedure TkvBooleanValue.SetAsBoolean(const A: Boolean);
begin
  FValue := A;
end;

function TkvBooleanValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Boolean;
end;

function TkvBooleanValue.GetSerialSize: Integer;
begin
  Result := SizeOf(Boolean);
end;

function TkvBooleanValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(Byte) then
    raise EkvValue.Create(SInvalidBufferSize);
  PByte(@Buf)^ := Ord(FValue);
  Result := SizeOf(Byte);
end;

function TkvBooleanValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(Byte) then
    raise EkvValue.Create(SInvalidBufferSize);
  FValue := PByte(@Buf)^ <> 0;
  Result := SizeOf(Byte);
end;

procedure TkvBooleanValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
begin
  Buf := @FValue;
  BufSize := SizeOf(Byte);
end;



{ TkvDateTimeValue }

class function TkvDateTimeValue.CreateInstance: AkvValue;
begin
  Result := TkvDateTimeValue.Create;
end;

constructor TkvDateTimeValue.Create;
begin
  inherited Create;
end;

constructor TkvDateTimeValue.Create(const Value: TDateTime);
begin
  inherited Create;
  FValue := Value;
end;

function TkvDateTimeValue.Duplicate: AkvValue;
begin
  Result := TkvDateTimeValue.Create(FValue);
end;

function TkvDateTimeValue.GetAsString: String;
begin
  Result := DateTimeToStr(FValue);
end;

function TkvDateTimeValue.GetAsScript: String;
begin
  Result := 'DATETIME("' + GetAsString + '")';
end;

function TkvDateTimeValue.GetAsDateTime: TDateTime;
begin
  Result := FValue;
end;

procedure TkvDateTimeValue.SetAsDateTime(const A: TDateTime);
begin
  FValue := A;
end;

function TkvDateTimeValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_DateTime;
end;

function TkvDateTimeValue.GetSerialSize: Integer;
begin
  Result := SizeOf(TDateTime);
end;

function TkvDateTimeValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(TDateTime) then
    raise EkvValue.Create(SInvalidBufferSize);
  PDateTime(@Buf)^ := FValue;
  Result := SizeOf(TDateTime);
end;

function TkvDateTimeValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(TDateTime) then
    raise EkvValue.Create(SInvalidBufferSize);
  FValue := PDateTime(@Buf)^;
  Result := SizeOf(TDateTime);
end;

procedure TkvDateTimeValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
begin
  Buf := @FValue;
  BufSize := SizeOf(TDateTime);
end;



{ TkvBinaryValue }

class function TkvBinaryValue.CreateInstance: AkvValue;
begin
  Result := TkvBinaryValue.Create;
end;

constructor TkvBinaryValue.Create;
begin
  inherited Create;
end;

constructor TkvBinaryValue.Create(const Value: kvByteArray);
begin
  inherited Create;
  FValue := Copy(Value);
end;

constructor TkvBinaryValue.Create(const Value: Byte);
begin
  inherited Create;
  SetLength(FValue, 1);
  FValue[0] := Value;
end;

function TkvBinaryValue.Duplicate: AkvValue;
begin
  Result := TkvBinaryValue.Create(FValue);
end;

function TkvBinaryValue.GetAsString: String;
var
  I, L : Integer;
  S : String;
  P : PChar;
begin
  L := Length(FValue);
  SetLength(S, L);
  P := PChar(S);
  for I := 0 to L - 1 do
    begin
      P^ := WideChar(FValue[I]);
      Inc(P);
    end;
  Result := S;
end;

function TkvBinaryValue.GetAsScript: String;
var
  S : String;
  I : Integer;
begin
  S := '';
  for I := 0 to Length(FValue) - 1 do
    begin
      if I > 0 then
        S := S + ' + ';
      S := S + 'BYTE(' + IntToStr(FValue[I]) + ')';
    end;
  Result := S;
end;

function TkvBinaryValue.GetAsBinary: kvByteArray;
begin
  Result := FValue;
end;

procedure TkvBinaryValue.SetAsString(const A: String);
var
  I, L : Integer;
  C : WideChar;
begin
  L := Length(A);
  SetLength(FValue, L);
  for I := 0 to L - 1 do
    begin
      C := A.Chars[I];
      if Ord(C) > $FF then
        raise EkvValue.Create('Type conversion error: Not a valid binary string');
      FValue[I] := Byte(Ord(C));
    end;
end;

procedure TkvBinaryValue.SetAsBinary(const A: kvByteArray);
begin
  FValue := Copy(A);
end;

function TkvBinaryValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Binary;
end;

function TkvBinaryValue.GetSerialSize: Integer;
var
  L : Integer;
begin
  L := Length(FValue);
  Result := kvVarWord32EncodedSize(L) + L;
end;

function TkvBinaryValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
var
  L : Int32;
  N : Integer;
  M : Integer;
  P : PByte;
begin
  L := Length(FValue);
  N := kvVarWord32EncodedSize(L);
  M := N + L;
  if BufSize < M then
    raise EkvValue.Create(SInvalidBufferSize);
  P := @Buf;
  kvVarWord32EncodeBuf(L, P^, N);
  Inc(P, N);
  if L > 0 then
    Move(FValue[0], P^, L);
  Result := M;
end;

function TkvBinaryValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  N : Integer;
  Len : Word32;
  M : Int32;
  S : kvByteArray;
begin
  P := @Buf;
  L := BufSize;

  N := kvVarWord32DecodeBuf(P^, L, Len);
  Dec(L, N);
  Inc(P, N);
  if Int32(Len) < 0 then
    raise EkvValue.Create('Invalid buffer: string length');

  M := Int32(Len);
  if L < M then
    raise EkvValue.Create(SInvalidBufferSize);
  Dec(L, M);
  SetLength(S, Len);
  if Len > 0 then
    Move(P^, S[0], M);
  FValue := S;

  Result := BufSize - L;
end;

procedure TkvBinaryValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
var
  L : Integer;
begin
  L := Length(FValue);
  if L = 0 then
    begin
      Buf := nil;
      BufSize := 0;
    end
  else
    begin
      Buf := @FValue[0];
      BufSize := L;
    end;
end;



{ TkvDecimal128Value }

class function TkvDecimal128Value.CreateInstance: AkvValue;
begin
  Result := TkvDecimal128Value.Create;
end;

constructor TkvDecimal128Value.Create;
begin
  inherited Create;
  SDecimal128InitZero(FValue);
end;

constructor TkvDecimal128Value.Create(const Value: SDecimal128);
begin
  inherited Create;
  SDecimal128InitSDecimal128(FValue, Value);
end;

function TkvDecimal128Value.Duplicate: AkvValue;
begin
  Result := TkvDecimal128Value.Create(FValue);
end;

function TkvDecimal128Value.GetAsString: String;
begin
  Result := SDecimal128ToStr(FValue);
end;

function TkvDecimal128Value.GetAsInteger: Int64;
begin
  Result := SDecimal128ToInt64(FValue);
end;

function TkvDecimal128Value.GetAsDecimal128: SDecimal128;
begin
  SDecimal128InitSDecimal128(Result, FValue);
end;

function TkvDecimal128Value.GetAsFloat: Double;
begin
  Result := SDecimal128ToFloat(FValue);
end;

procedure TkvDecimal128Value.SetAsString(const A: String);
begin
  if TryStrToSDecimal128(A, FValue) <> dceNoError then
    raise EkvValue.Create('Type conversion error: Not a valid decimal string');
end;

procedure TkvDecimal128Value.SetAsInteger(const A: Int64);
begin
  SDecimal128InitInt64(FValue, A);
end;

procedure TkvDecimal128Value.SetAsFloat(const A: Double);
begin
  SDecimal128InitFloat(FValue, A);
end;

procedure TkvDecimal128Value.SetAsDecimal128(const A: SDecimal128);
begin
  SDecimal128InitSDecimal128(FValue, A);
end;

function TkvDecimal128Value.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Decimal128;
end;

function TkvDecimal128Value.GetSerialSize: Integer;
begin
  Result := SizeOf(SDecimal128);
end;

function TkvDecimal128Value.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(SDecimal128) then
    raise EkvValue.Create(SInvalidBufferSize);
  SDecimal128InitSDecimal128(PSDecimal128(@Buf)^, FValue);
  Result := SizeOf(SDecimal128);
end;

function TkvDecimal128Value.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  if BufSize < SizeOf(SDecimal128) then
    raise EkvValue.Create(SInvalidBufferSize);
  SDecimal128InitSDecimal128(FValue, PSDecimal128(@Buf)^);
  Result := SizeOf(SDecimal128);
end;



{ TkvNullValue }

class function TkvNullValue.CreateInstance: AkvValue;
begin
  Result := TkvNullValue.Create;
end;

function TkvNullValue.Duplicate: AkvValue;
begin
  Result := TkvNullValue.Create;
end;

function TkvNullValue.GetAsString: String;
begin
  Result := 'null';
end;

function TkvNullValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Null;
end;

function TkvNullValue.GetSerialSize: Integer;
begin
  Result := 0;
end;

function TkvNullValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
begin
  Result := 0;
end;

function TkvNullValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
begin
  Result := 0;
end;

procedure TkvNullValue.GetDataBuf(out Buf: Pointer; out BufSize: Integer);
begin
  Buf := nil;
  BufSize := 0;
end;



{ TkvListValue }

class function TkvListValue.CreateInstance: AkvValue;
begin
  Result := TkvListValue.Create;
end;

destructor TkvListValue.Destroy;
var
  I : Integer;
begin
  for I := Length(FValue) - 1 downto 0 do
    FreeAndNil(FValue[I]);
  inherited Destroy;
end;

function TkvListValue.GetCount: Integer;
begin
  Result := Length(FValue);
end;

function TkvListValue.Duplicate: AkvValue;
var
  R : TkvListValue;
  I : Integer;
begin
  R := TkvListValue.Create;
  for I := 0 to Length(FValue) - 1 do
    R.Add(FValue[I].Duplicate);
  Result := R;
end;

function TkvListValue.GetAsString: String;
var
  S : String;
  I : Integer;
begin
  S := '[';
  for I := 0 to Length(FValue) - 1 do
    begin
      if I > 0 then
        S := S + ',';
      S := S + FValue[I].GetAsScript;
    end;
  S := S + ']';
  Result := S;
end;

function TkvListValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_List;
end;

function TkvListValue.GetSerialSize: Integer;
var
  L : Integer;
  R : Integer;
  I : Integer;
begin
  L := Length(FValue);
  R := kvVarWord32EncodedSize(L);
  for I := 0 to L - 1 do
    Inc(R, 1 + FValue[I].GetSerialSize);
  Result := R;
end;

class function TkvListValue.EncodeEntry(var Buf; const BufSize: Integer; const Value: AkvValue): Integer;
var
  P : PByte;
  L : Integer;
  N : Integer;
begin
  P := @Buf;
  L := BufSize;
  if L < 1 then
    raise EkvValue.Create(SInvalidBufferSize);
  P^ := Value.GetTypeId;
  Inc(P);
  Dec(L);
  N := Value.GetSerialBuf(P^, L);
  Dec(L, N);
  Result := BufSize - L;
end;

function TkvListValue.EncodeEntries(var Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  M : Int32;
  I : Integer;
  V : AkvValue;
  N : Integer;
begin
  P := @Buf;
  L := BufSize;
  M := Length(FValue);
  for I := 0 to M - 1 do
    begin
      V := FValue[I];
      N := EncodeEntry(P^, L, V);
      Inc(P, N);
      Dec(L, N);
    end;
  Result := BufSize - L;
end;

function TkvListValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  M : Int32;
  F : Integer;
begin
  P := @Buf;
  L := BufSize;
  M := Length(FValue);
  F := kvVarWord32EncodeBuf(M, P^, L);
  Inc(P, F);
  Dec(L, F);
  F := EncodeEntries(P^, L);
  Dec(L, F);
  Result := BufSize - L;
end;

function TkvListValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  F : Integer;
  N : Integer;
  Cnt : Word32;
  I : Integer;
  TypId : Byte;
  Val : AkvValue;
begin
  P := @Buf;
  L := BufSize;
  F := kvVarWord32DecodeBuf(P^, L, Cnt);
  if Int32(Cnt) < 0 then
    raise EKvValue.Create('Invalid buffer: List count invalid');
  Inc(P, F);
  Dec(L, F);

  SetLength(FValue, Cnt);
  for I := 0 to Int32(Cnt) - 1 do
    begin
      if L < 1 then
        raise EkvValue.Create(SInvalidBufferSize);
      TypId := P^;
      Inc(P);
      Dec(L);

      Val := kvCreateValueFromTypeId(TypId);
      N := Val.PutSerialBuf(P^, L);
      Inc(P, N);
      Dec(L, N);

      FValue[I] := Val;
    end;

  Result := BufSize - L;
end;

procedure TkvListValue.Add(const Value: AkvValue);
var
  L : Integer;
begin
  L := Length(FValue);
  SetLength(FValue, L + 1);
  FValue[L] := Value;
end;

procedure TkvListValue.AddList(const List: TkvListValue);
var
  L, N, I : Integer;
begin
  N := Length(List.FValue);
  if N = 0 then
    exit;
  L := Length(FValue);
  SetLength(FValue, L + N);
  for I := 0 to N - 1 do
    FValue[L + I] := List.FValue[I].Duplicate;
end;

function TkvListValue.GetValue(const Index: Integer): AkvValue;
begin
  if (Index < 0) or (Index >= Length(FValue)) then
    raise EkvValue.Create('List index out of range');
  Result := FValue[Index];
end;

procedure TkvListValue.SetValue(const Index: Integer; const Value: AkvValue);
begin
  if (Index < 0) or (Index >= Length(FValue)) then
    raise EkvValue.Create('List index out of range');
  FValue[Index].Free;
  FValue[Index] := Value;
end;

procedure TkvListValue.DeleteValue(const Index: Integer);
var
  I, L : Integer;
begin
  L := Length(FValue);
  if (Index < 0) or (Index >= L) then
    raise EkvValue.Create('List index out of range');
  FValue[Index].Free;
  for I := Index to L - 2 do
    FValue[I] := FValue[I + 1];
  SetLength(FValue, L - 1);
end;

procedure TkvListValue.InsertValue(const Index: Integer; const Value: AkvValue);
var
  I, L : Integer;
begin
  L := Length(FValue);
  if (Index < 0) or (Index > L) then
    raise EkvValue.Create('List index out of range');
  SetLength(FValue, L + 1);
  for I := L downto Index + 1 do
    FValue[I] := FValue[I - 1];
  FValue[Index] := Value;
end;

procedure TkvListValue.AppendValue(const Value: AkvValue);
var
  L : Integer;
begin
  L := Length(FValue);
  SetLength(FValue, L + 1);
  FValue[L] := Value;
end;

function TkvListValue.HasValue(const Value: AkvValue): Boolean;
var
  I : Integer;
begin
  for I := 0 to Length(FValue) - 1 do
    if FValue[I].ClassType = Value.ClassType then
      if ValueOpCompare(FValue[I], Value) = 0 then
        begin
          Result := True;
          exit;
        end;
  Result := False;
end;



{ TkvDictionaryValue }

class function TkvDictionaryValue.CreateInstance: AkvValue;
begin
  Result := TkvDictionaryValue.Create;
end;

constructor TkvDictionaryValue.Create;
begin
  inherited Create;
  FValue := TkvStringHashList.Create(True, False, True);
end;

destructor TkvDictionaryValue.Destroy;
begin
  FreeAndNil(FValue);
  inherited Destroy;
end;

procedure TkvDictionaryValue.Clear;
begin
  FValue.Clear;
end;

function TkvDictionaryValue.Duplicate: AkvValue;
var
  R : TkvDictionaryValue;
  Itr : TkvStringHashListIterator;
  Itm : PkvStringHashListItem;
begin
  R := CreateInstance as TkvDictionaryValue;
  Itm := FValue.IterateFirst(Itr);
  while Assigned(Itm) do
    begin
      R.Add(Itm^.Key, AkvValue(Itm^.Value).Duplicate);
      Itm := FValue.IterateNext(Itr);
    end;
  Result := R;
end;

function TkvDictionaryValue.GetAsString: String;
var
  S : String;
  F : Boolean;
  Itr : TkvStringHashListIterator;
  Itm : PkvStringHashListItem;
begin
  S := '{';
  F := True;
  Itm := FValue.IterateFirst(Itr);
  while Assigned(Itm) do
    begin
      if F then
        F := False
      else
        S := S + ',';
      S := S + Itm^.Key + ':' + AkvValue(Itm^.Value).GetAsScript;
      Itm := FValue.IterateNext(Itr);
    end;
  S := S + '}';
  Result := S;
end;

function TkvDictionaryValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Dictionary;
end;

function TkvDictionaryValue.GetSerialSize: Integer;
var
  L : Integer;
  R : Integer;
begin
  L := FValue.Count;
  R := kvVarWord32EncodedSize(L);
  Inc(R, EncodedEntriesSize);
  Result := R;
end;

class function TkvDictionaryValue.EncodeEntry(var Buf; const BufSize: Integer;
               const Key: String; const Value: AkvValue): Integer;
var
  P : PByte;
  L : Integer;
  T : Integer;
  F : Integer;
  N : Integer;
begin
  P := @Buf;
  L := BufSize;
  T := Length(Key);
  F := kvVarWord32EncodeBuf(T, P^, L);
  Inc(P, F);
  Dec(L, F);
  N := T * SizeOf(Char);
  if L < N then
    raise EkvValue.Create(SInvalidBufferSize);
  if T > 0 then
    Move(PChar(Key)^, P^, N);
  Inc(P, N);
  Dec(L, N);
  if L < 1 then
    raise EkvValue.Create(SInvalidBufferSize);
  P^ := Value.GetTypeId;
  Inc(P);
  Dec(L);
  N := Value.GetSerialBuf(P^, L);
  Dec(L, N);
  Result := BufSize - L;
end;

function TkvDictionaryValue.EncodeEntries(var Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  F : Integer;
  Itr : TkvStringHashListIterator;
  Itm : PkvStringHashListItem;
begin
  P := @Buf;
  L := BufSize;
  Itm := FValue.IterateFirst(Itr);
  while Assigned(Itm) do
    begin
      F := EncodeEntry(P^, L, Itm^.Key, AkvValue(Itm^.Value));
      Inc(P, F);
      Dec(L, F);
      Itm := FValue.IterateNext(Itr);
    end;
  Result := BufSize - L;
end;

function TkvDictionaryValue.EncodedEntriesSize: Integer;
var
  R : Integer;
  DictKey : String;
  DictKeyLen : Integer;
  Itr : TkvStringHashListIterator;
  Itm : PkvStringHashListItem;
begin
  R := 0;
  Itm := FValue.IterateFirst(Itr);
  while Assigned(Itm) do
    begin
      DictKey := Itm^.Key;
      DictKeyLen := Length(DictKey);
      Inc(R, DictKeyLen * SizeOf(Char) + kvVarWord32EncodedSize(DictKeyLen));
      Inc(R, AkvValue(Itm^.Value).SerialSize + 1);
      Itm := FValue.IterateNext(Itr);
    end;
  Result := R;
end;

function TkvDictionaryValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  M : Int32;
  F : Integer;
begin
  P := @Buf;
  L := BufSize;
  M := FValue.Count;
  F := kvVarWord32EncodeBuf(M, P^, L);
  Inc(P, F);
  Dec(L, F);
  F := EncodeEntries(P^, L);
  Dec(L, F);
  Result := BufSize - L;
end;

function TkvDictionaryValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  N : Integer;
  F : Integer;
  Cnt : Word32;
  I : Integer;
  TypId : Byte;
  Val : AkvValue;
  KeyLen : Word32;
  Key : String;
  M : Integer;
begin
  P := @Buf;
  L := BufSize;
  F := kvVarWord32DecodeBuf(P^, L, Cnt);
  Inc(P, F);
  Dec(L, F);
  if Int32(Cnt) < 0 then
    raise EkvValue.Create('Invalid buffer: List count invalid');

  FValue.Clear;
  for I := 0 to Int32(Cnt) - 1 do
    begin
      F := kvVarWord32DecodeBuf(P^, L, KeyLen);
      Inc(P, F);
      Dec(L, F);
      if Int32(KeyLen) < 0 then
        raise EkvValue.Create('Invalid buffer: Key length invalid');

      M := KeyLen * SizeOf(Char);
      if L < M then
        raise EkvValue.Create(SInvalidBufferSize);
      SetLength(Key, KeyLen);
      if KeyLen > 0 then
        Move(P^, PChar(Key)^, M);
      Inc(P, M);
      Dec(L, M);

      if L < 1 then
        raise EkvValue.Create(SInvalidBufferSize);
      TypId := P^;
      Inc(P);
      Dec(L);

      Val := kvCreateValueFromTypeId(TypId);
      N := Val.PutSerialBuf(P^, L);
      Inc(P, N);
      Dec(L, N);

      FValue.Add(Key, Val);
    end;

  Result := BufSize - L;
end;

procedure TkvDictionaryValue.Add(const Key: String; const Value: AkvValue);
begin
  Assert(Assigned(Value));
  FValue.Add(Key, Value);
end;

procedure TkvDictionaryValue.AddItems(const Items: TkvDictionaryValue);
var
  It : TkvDictionaryValueIterator;
  Key : String;
  Value : AkvValue;
begin
  if Items.IterateFirst(It) then
    repeat
      Items.IteratorGetKeyValue(It, Key, Value);
      Add(Key, Value.Duplicate);
    until not Items.IterateNext(It);
end;

procedure TkvDictionaryValue.AddString(const Key: String; const Value: String);
begin
  Add(Key, TkvStringValue.Create(Value));
end;

procedure TkvDictionaryValue.AddBoolean(const Key: String; const Value: Boolean);
begin
  Add(Key, TkvBooleanValue.Create(Value));
end;

procedure TkvDictionaryValue.AddFloat(const Key: String; const Value: Double);
begin
  Add(Key, TkvFloatValue.Create(Value));
end;

procedure TkvDictionaryValue.AddInteger(const Key: String; const Value: Int64);
begin
  Add(Key, TkvIntegerValue.Create(Value));
end;

procedure TkvDictionaryValue.AddDateTime(const Key: String; const Value: TDateTime);
begin
  Add(Key, TkvDateTimeValue.Create(Value));
end;

procedure TkvDictionaryValue.AddBinary(const Key: String; const Value: kvByteArray);
begin
  Add(Key, TkvBinaryValue.Create(Value));
end;

procedure TkvDictionaryValue.AddNull(const Key: String);
begin
  Add(Key, TkvNullValue.Create);
end;

function TkvDictionaryValue.Exists(const Key: String): Boolean;
begin
  Result := FValue.KeyExists(Key);
end;

function TkvDictionaryValue.IsValueNull(const Key: String): Boolean;
var
  V : TObject;
begin
  if not FValue.GetValue(Key, V) then
    raise EkvValue.CreateFmt('Dictionary key not found: %s', [Key]);
  Result := AkvValue(V) is TkvNullValue;
end;

function TkvDictionaryValue.GetValue(const Key: String): AkvValue;
var
  V : TObject;
begin
  if not FValue.GetValue(Key, V) then
    raise EkvValue.CreateFmt('Dictionary key not found: %s', [Key]);
  Result := AkvValue(V);
end;

function TkvDictionaryValue.GetValueAsString(const Key: String): String;
begin
  Result := GetValue(Key).AsString;
end;

function TkvDictionaryValue.GetValueAsBoolean(const Key: String): Boolean;
begin
  Result := GetValue(Key).AsBoolean;
end;

function TkvDictionaryValue.GetValueAsFloat(const Key: String): Double;
begin
  Result := GetValue(Key).AsFloat;
end;

function TkvDictionaryValue.GetValueAsInteger(const Key: String): Int64;
begin
  Result := GetValue(Key).AsInteger;
end;

function TkvDictionaryValue.GetValueAsDateTime(const Key: String): TDateTime;
begin
  Result := GetValue(Key).AsDateTime;
end;

function TkvDictionaryValue.GetValueAsBinary(const Key: String): kvByteArray;
begin
  Result := GetValue(Key).AsBinary;
end;

function TkvDictionaryValue.GetValueIfExists(const Key: String): AkvValue;
var
  V : TObject;
begin
  if not FValue.GetValue(Key, V) then
    Result := nil
  else
    Result := AkvValue(V);
end;

function TkvDictionaryValue.GetValueAsStringDef(const Key: String; const ADefault: String): String;
var
  V : AkvValue;
begin
  V := GetValueIfExists(Key);
  if not Assigned(V) then
    Result := ADefault
  else
    try
      Result := V.AsString;
    except
      Result := ADefault;
    end;
end;

function TkvDictionaryValue.GetValueAsBooleanDef(const Key: String; const ADefault: Boolean): Boolean;
var
  V : AkvValue;
begin
  V := GetValueIfExists(Key);
  if not Assigned(V) then
    Result := ADefault
  else
    try
      Result := V.AsBoolean;
    except
      Result := ADefault;
    end;
end;

function TkvDictionaryValue.GetValueAsFloatDef(const Key: String; const ADefault: Double): Double;
var
  V : AkvValue;
begin
  V := GetValueIfExists(Key);
  if not Assigned(V) then
    Result := ADefault
  else
    try
      Result := V.AsFloat;
    except
      Result := ADefault;
    end;
end;

function TkvDictionaryValue.GetValueAsIntegerDef(const Key: String; const ADefault: Int64): Int64;
var
  V : AkvValue;
begin
  V := GetValueIfExists(Key);
  if not Assigned(V) then
    Result := ADefault
  else
    try
      Result := V.AsInteger;
    except
      Result := ADefault;
    end;
end;

function TkvDictionaryValue.GetValueAsDateTimeDef(const Key: String; const ADefault: TDateTime): TDateTime;
var
  V : AkvValue;
begin
  V := GetValueIfExists(Key);
  if not Assigned(V) then
    Result := ADefault
  else
    try
      Result := V.AsDateTime;
    except
      Result := ADefault;
    end;
end;

function TkvDictionaryValue.GetValueAsBinaryDef(const Key: String; const ADefault: kvByteArray): kvByteArray;
var
  V : AkvValue;
begin
  V := GetValueIfExists(Key);
  if not Assigned(V) then
    Result := ADefault
  else
    try
      Result := V.AsBinary;
    except
      Result := ADefault;
    end;
end;

procedure TkvDictionaryValue.SetValueAsString(const Key: String; const Value: String);
begin
  GetValue(Key).AsString := Value;
end;

procedure TkvDictionaryValue.SetValueAsBoolean(const Key: String; const Value: Boolean);
begin
  GetValue(Key).AsBoolean := Value;
end;

procedure TkvDictionaryValue.SetValueAsInteger(const Key: String; const Value: Int64);
begin
  GetValue(Key).AsInteger := Value;
end;

procedure TkvDictionaryValue.SetValueAsDateTime(const Key: String; const Value: TDateTime);
begin
  GetValue(Key).AsDateTime := Value;
end;

procedure TkvDictionaryValue.SetValueAsBinary(const Key: String; const Value: kvByteArray);
begin
  GetValue(Key).AsBinary := Value;
end;

procedure TkvDictionaryValue.SetValue(const Key: String; const Value: AkvValue);
begin
  Assert(Assigned(Value));
  FValue.SetValue(Key, Value);
end;

procedure TkvDictionaryValue.SetValueString(const Key: String; const Value: String);
begin
  SetValue(Key, TkvStringValue.Create(Value));
end;

procedure TkvDictionaryValue.SetValueBoolean(const Key: String; const Value: Boolean);
begin
  SetValue(Key, TkvBooleanValue.Create(Value));
end;

procedure TkvDictionaryValue.SetValueInteger(const Key: String; const Value: Int64);
begin
  SetValue(Key, TkvIntegerValue.Create(Value));
end;

procedure TkvDictionaryValue.SetValueDateTime(const Key: String; const Value: TDateTime);
begin
  SetValue(Key, TkvDateTimeValue.Create(Value));
end;

procedure TkvDictionaryValue.SetValueBinary(const Key: String; const Value: kvByteArray);
begin
  SetValue(Key, TkvBinaryValue.Create(Value));
end;

procedure TkvDictionaryValue.SetValueNull(const Key: String);
begin
  SetValue(Key, TkvNullValue.Create);
end;

procedure TkvDictionaryValue.AddOrSetValue(const Key: String; const Value: AkvValue);
begin
  Assert(Assigned(Value));
  FValue.AddOrSet(Key, Value);
end;

procedure TkvDictionaryValue.AddOrSetValueString(const Key: String; const Value: String);
begin
  AddOrSetValue(Key, TkvStringValue.Create(Value));
end;

procedure TkvDictionaryValue.AddOrSetValueBoolean(const Key: String; const Value: Boolean);
begin
  AddOrSetValue(Key, TkvBooleanValue.Create(Value));
end;

procedure TkvDictionaryValue.AddOrSetValueInteger(const Key: String; const Value: Int64);
begin
  AddOrSetValue(Key, TkvIntegerValue.Create(Value));
end;

procedure TkvDictionaryValue.AddOrSetValueDateTime(const Key: String; const Value: TDateTime);
begin
  AddOrSetValue(Key, TkvDateTimeValue.Create(Value));
end;

procedure TkvDictionaryValue.AddOrSetValueBinary(const Key: String; const Value: kvByteArray);
begin
  AddOrSetValue(Key, TkvBinaryValue.Create(Value));
end;

procedure TkvDictionaryValue.DeleteKey(const Key: String);
begin
  FValue.DeleteKey(Key);
end;

function TkvDictionaryValue.ReleaseKey(const Key: String): TObject;
begin
  FValue.RemoveKey(Key, Result);
end;

function TkvDictionaryValue.IterateFirst(out Iterator: TkvDictionaryValueIterator): Boolean;
begin
  Iterator.Itm := FValue.IterateFirst(Iterator.Itr);
  Result := Assigned(Iterator.Itm);
end;

function TkvDictionaryValue.IterateNext(out Iterator: TkvDictionaryValueIterator): Boolean;
begin
  Iterator.Itm := FValue.IterateNext(Iterator.Itr);
  Result := Assigned(Iterator.Itm);
end;

procedure TkvDictionaryValue.IteratorGetKeyValue(
          const Iterator: TkvDictionaryValueIterator;
          out Key: String; out Value: AkvValue);
begin
  if not Assigned(Iterator.Itm) then
    raise EkvValue.Create('Iterator has no value');
  Key := Iterator.Itm^.Key;
  Value := AkvValue(Iterator.Itm.Value);
end;

function TkvDictionaryValue.GetCount: Integer;
begin
  Result := FValue.Count;
end;



{ TkvSetValue }

class function TkvSetValue.CreateInstance: AkvValue;
begin
  Result := TkvSetValue.Create;
end;

constructor TkvSetValue.Create;
begin
  inherited Create;
  FValue := TkvStringHashList.Create(True, False, False);
end;

constructor TkvSetValue.Create(const Value: TkvStringHashList);
begin
  inherited Create;
  FValue := Value;
end;

destructor TkvSetValue.Destroy;
begin
  FreeAndNil(FValue);
  inherited Destroy;
end;

function TkvSetValue.Duplicate: AkvValue;
var
  V : TkvStringHashList;
  P : PkvStringHashListItem;
  I : TkvStringHashListIterator;
begin
  V := TkvStringHashList.Create(True, False, False);
  P := FValue.IterateFirst(I);
  while Assigned(P) do
    begin
      V.Add(P^.Key, nil);
      P := FValue.IterateNext(I);
    end;
  Result := TkvSetValue.Create(V);
end;

function TkvSetValue.GetAsString: String;
var
  S : String;
  P : PkvStringHashListItem;
  I : TkvStringHashListIterator;
  F : Boolean;
begin
  S := 'SETOF([';
  P := FValue.IterateFirst(I);
  F := True;
  while Assigned(P) do
    begin
      if F then
        F := False
      else
        S := S + ',';
      S := S + '"' + ReplaceStr(P^.Key, '"', '""') + '"';
      P := FValue.IterateNext(I);
    end;
  S := S + '])';
  Result := S;
end;

function TkvSetValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Set;
end;

function TkvSetValue.GetSerialSize: Integer;
var
  L : Integer;
  R : Integer;
  P : PkvStringHashListItem;
  I : TkvStringHashListIterator;
begin
  L := FValue.Count;
  R := kvVarWord32EncodedSize(L);
  P := FValue.IterateFirst(I);
  while Assigned(P) do
    begin
      L := Length(P^.Key);
      Inc(R, kvVarWord32EncodedSize(L) + L * SizeOf(Char));
      P := FValue.IterateNext(I);
    end;
  Result := R;
end;

function TkvSetValue.GetSerialBuf(var Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  M : Int32;
  F : Integer;
  I : Integer;
  N : Integer;
  T : Int32;
  Itm : PkvStringHashListItem;
  Itr : TkvStringHashListIterator;
begin
  P := @Buf;
  L := BufSize;
  M := FValue.Count;
  F := kvVarWord32EncodeBuf(M, P^, L);
  Inc(P, F);
  Dec(L, F);
  Itm := FValue.IterateFirst(Itr);
  for I := 0 to M - 1 do
    begin
      Assert(Assigned(Itm));
      T := Length(Itm^.Key);
      F := kvVarWord32EncodeBuf(T, P^, L);
      Inc(P, F);
      Dec(L, F);
      N := T * SizeOf(Char);
      if L < N then
        raise EkvValue.Create(SInvalidBufferSize);
      if T > 0 then
        Move(PChar(Itm^.Key)^, P^, N);
      Inc(P, N);
      Dec(L, N);
      Itm := FValue.IterateNext(Itr);
    end;
  Result := BufSize - L;
end;

function TkvSetValue.PutSerialBuf(const Buf; const BufSize: Integer): Integer;
var
  P : PByte;
  L : Integer;
  F : Integer;
  Cnt : Word32;
  I : Integer;
  KeyLen : Word32;
  Key : String;
  M : Integer;
begin
  P := @Buf;
  L := BufSize;
  F := kvVarWord32DecodeBuf(P^, L, Cnt);
  Inc(P, F);
  Dec(L, F);
  if Int32(Cnt) < 0 then
    raise EkvValue.Create('Invalid buffer: Set count invalid');

  FValue.Clear;
  for I := 0 to Int32(Cnt) - 1 do
    begin
      F := kvVarWord32DecodeBuf(P^, L, KeyLen);
      Inc(P, F);
      Dec(L, F);
      if Int32(KeyLen) < 0 then
        raise EkvValue.Create('Invalid buffer: Key length invalid');

      M := KeyLen * SizeOf(Char);
      if L < M then
        raise EkvValue.Create(SInvalidBufferSize);
      SetLength(Key, KeyLen);
      if KeyLen > 0 then
        Move(P^, PChar(Key)^, M);
      Inc(P, M);
      Dec(L, M);

      FValue.Add(Key, nil);
    end;

  Result := BufSize - L;
end;

procedure TkvSetValue.Clear;
begin
  FValue.Clear;
end;

procedure TkvSetValue.Add(const Key: String);
begin
  if FValue.KeyExists(Key) then
    exit;
  FValue.Add(Key, nil);
end;

procedure TkvSetValue.AddSet(const Value: TkvSetValue);
var
  P : PkvStringHashListItem;
  I : TkvStringHashListIterator;
begin
  P := Value.FValue.IterateFirst(I);
  while Assigned(P) do
    begin
      Add(P^.Key);
      P := Value.FValue.IterateNext(I);
    end;
end;

function TkvSetValue.Exists(const Key: String): Boolean;
begin
  Result := FValue.KeyExists(Key);
end;

procedure TkvSetValue.DeleteKey(const Key: String);
begin
  if not FValue.KeyExists(Key) then
    exit;
  FValue.DeleteKey(Key);
end;

procedure TkvSetValue.DeleteSet(const Value: TkvSetValue);
var
  P : PkvStringHashListItem;
  I : TkvStringHashListIterator;
begin
  P := Value.FValue.IterateFirst(I);
  while Assigned(P) do
    begin
      DeleteKey(P^.Key);
      P := Value.FValue.IterateNext(I);
    end;
end;



{ TkvFolderValue }

class function TkvFolderValue.CreateInstance: AkvValue;
begin
  Result := TkvFolderValue.Create;
end;

function TkvFolderValue.GetTypeId: Byte;
begin
  Result := KV_Value_TypeId_Folder;
end;



{ kvByteArray helpers }

function kvByteArrayAppend(const A, B: kvByteArray): kvByteArray;
var
  L1 : Integer;
  L2 : Integer;
  L : Integer;
  R : kvByteArray;
begin
  L1 := Length(A);
  L2 := Length(B);
  L := L1 + L2;
  SetLength(R, L);
  if L1 > 0 then
    Move(A[0], R[0], L1);
  if L2 > 0 then
    Move(B[0], R[L1], L2);
  Result := R;
end;

function kvByteArrayCompare(const A, B: kvByteArray): Integer;
var
  L1 : Integer;
  L2 : Integer;
  I : Integer;
  F, G : Byte;
begin
  L1 := Length(A);
  L2 := Length(B);
  if L1 < L2 then
    Result := -1
  else
  if L1 > L2 then
    Result := 1
  else
    begin
      for I := 0 to L1 - 1 do
        begin
          F := A[I];
          G := B[I];
          if F < G then
            begin
              Result := -1;
              exit;
            end
          else
          if F > G then
            begin
              Result := 1;
              exit;
            end;
        end;
      Result := 0;
    end;
end;



{ Value operators }

function ValueOpPlus(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
  S : TkvSetValue;
  L : TkvListValue;
  D : SDecimal128;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if TA = KV_Value_TypeId_Set then
    if TB = KV_Value_TypeId_Set then
      begin
        S := A.Duplicate as TkvSetValue;
        S.AddSet(TkvSetValue(B));
        Result := S;
      end
    else
      begin
        S := A.Duplicate as TkvSetValue;
        S.Add(B.GetAsString);
        Result := S;
      end
  else
  if TB = KV_Value_TypeId_Set then
    begin
      S := B.Duplicate as TkvSetValue;
      S.Add(A.GetAsString);
      Result := S;
    end
  else
  if TA = KV_Value_TypeId_List then
    if TB = KV_Value_TypeId_List then
      begin
        L := A.Duplicate as TkvListValue;
        L.AddList(TkvListValue(B));
        Result := L;
      end
    else
      begin
        L := A.Duplicate as TkvListValue;
        L.Add(B.Duplicate);
        Result := L;
      end
  else
  if TB = KV_Value_TypeId_List then
    begin
      L := TkvListValue.Create;
      L.Add(A.Duplicate);
      L.AddList(TkvListValue(B));
      Result := L;
    end
  else
  if (TA = KV_Value_TypeId_DateTime) or (TB = KV_Value_TypeId_DateTime) then
    Result := TkvDateTimeValue.Create(A.GetAsDateTime + B.GetAsDateTime)
  else
  if (TA = KV_Value_TypeId_Binary) or (TB = KV_Value_TypeId_Binary) then
    Result := TkvBinaryValue.Create(kvByteArrayAppend(A.GetAsBinary, B.GetAsBinary))
  else
  if (TA = KV_Value_TypeId_String) or (TB = KV_Value_TypeId_String) then
    Result := TkvStringValue.Create(A.GetAsString + B.GetAsString)
  else
  if (TA = KV_Value_TypeId_Decimal128) or (TB = KV_Value_TypeId_Decimal128) then
    begin
      SDecimal128InitSDecimal128(D, A.AsDecimal128);
      SDecimal128AddSDecimal128(D, B.AsDecimal128);
      Result := TkvDecimal128Value.Create(D);
    end else
  if (TA = KV_Value_TypeId_Float) or (TB = KV_Value_TypeId_Float) then
    Result := TkvFloatValue.Create(A.GetAsFloat + B.GetAsFloat)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvIntegerValue.Create(A.GetAsInteger + B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot add types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpMinus(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
  S : TkvSetValue;
  D : SDecimal128;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if TA = KV_Value_TypeId_Set then
    if TB = KV_Value_TypeId_Set then
      begin
        S := A.Duplicate as TkvSetValue;
        S.DeleteSet(TkvSetValue(B));
        Result := S;
      end
    else
      begin
        S := A.Duplicate as TkvSetValue;
        S.DeleteKey(B.GetAsString);
        Result := S;
      end
  else
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if (TA = KV_Value_TypeId_Decimal128) or (TB = KV_Value_TypeId_Decimal128) then
    begin
      SDecimal128InitSDecimal128(D, A.AsDecimal128);
      SDecimal128SubtractSDecimal128(D, B.AsDecimal128);
      Result := TkvDecimal128Value.Create(D);
    end else
  if (TA = KV_Value_TypeId_Float) or (TB = KV_Value_TypeId_Float) then
    Result := TkvFloatValue.Create(A.GetAsFloat - B.GetAsFloat)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvIntegerValue.Create(A.GetAsInteger - B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot subtract types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpMultiply(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
  D : SDecimal128;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if (TA = KV_Value_TypeId_Decimal128) or (TB = KV_Value_TypeId_Decimal128) then
    begin
      SDecimal128InitSDecimal128(D, A.AsDecimal128);
      SDecimal128MultiplySDecimal128(D, B.AsDecimal128);
      Result := TkvDecimal128Value.Create(D);
    end else
  if (TA = KV_Value_TypeId_Float) or (TB = KV_Value_TypeId_Float) then
    Result := TkvFloatValue.Create(A.GetAsFloat * B.GetAsFloat)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvIntegerValue.Create(A.GetAsInteger * B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot multiply types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpDivide(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
  D : SDecimal128;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if (TA = KV_Value_TypeId_Decimal128) or (TB = KV_Value_TypeId_Decimal128) then
    begin
      SDecimal128InitSDecimal128(D, A.AsDecimal128);
      SDecimal128DivideSDecimal128(D, B.AsDecimal128);
      Result := TkvDecimal128Value.Create(D);
    end else
  if (TA = KV_Value_TypeId_Float) or (TB = KV_Value_TypeId_Float) then
    Result := TkvFloatValue.Create(A.GetAsFloat / B.GetAsFloat)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvFloatValue.Create(A.GetAsInteger / B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot divide types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpOR(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if (TA = KV_Value_TypeId_Boolean) or (TB = KV_Value_TypeId_Boolean) then
    Result := TkvBooleanValue.Create(A.GetAsBoolean or B.GetAsBoolean)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvIntegerValue.Create(A.GetAsInteger or B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot OR types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpXOR(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if (TA = KV_Value_TypeId_Boolean) or (TB = KV_Value_TypeId_Boolean) then
    Result := TkvBooleanValue.Create(A.GetAsBoolean xor B.GetAsBoolean)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvIntegerValue.Create(A.GetAsInteger xor B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot XOR types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpAND(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
begin
  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) or (TB = KV_Value_TypeId_Null) then
    Result := TkvNullValue.Create
  else
  if (TA = KV_Value_TypeId_Boolean) or (TB = KV_Value_TypeId_Boolean) then
    Result := TkvBooleanValue.Create(A.GetAsBoolean and B.GetAsBoolean)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := TkvIntegerValue.Create(A.GetAsInteger and B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot AND types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpNOT(const A: AkvValue): AkvValue;
var
  TA : Integer;
begin
  TA := A.TypeId;
  if TA = KV_Value_TypeId_Null then
    Result := TkvNullValue.Create
  else
  if TA = KV_Value_TypeId_Boolean then
    Result := TkvBooleanValue.Create(not A.GetAsBoolean)
  else
  if TA = KV_Value_TypeId_Integer then
    Result := TkvIntegerValue.Create(not A.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot NOT type: %s', [A.ClassName]);
end;

function CompareFloat(const A, B: Double): Integer;
begin
  if A < B then
    Result := -1
  else
  if A > B then
    Result := 1
  else
    Result := 0;
end;

function CompareInt(const A, B: Int64): Integer;
begin
  if A < B then
    Result := -1
  else
  if A > B then
    Result := 1
  else
    Result := 0;
end;

function CompareDateTime(const A, B: TDateTime): Integer;
begin
  if A < B then
    Result := -1
  else
  if A > B then
    Result := 1
  else
    Result := 0;
end;

function ValueOpCompare(const A, B: AkvValue): Integer;
var
  TA, TB : Byte;
begin
  Assert(Assigned(A));
  Assert(Assigned(B));

  TA := A.TypeId;
  TB := B.TypeId;
  if (TA = KV_Value_TypeId_Null) and (TB = KV_Value_TypeId_Null) then
    Result := 0
  else
  if TA = KV_Value_TypeId_Null then
    Result := -1
  else
  if TB = KV_Value_TypeId_Null then
    Result := 1
  else
  if (TA = KV_Value_TypeId_Decimal128) or (TB = KV_Value_TypeId_Decimal128) then
    Result := SDecimal128CompareSDecimal128(A.GetAsDecimal128, B.GetAsDecimal128)
  else
  if (TA = KV_Value_TypeId_Binary) or (TB = KV_Value_TypeId_Binary) then
    Result := kvByteArrayCompare(A.GetAsBinary, B.GetAsBinary)
  else
  if (TA = KV_Value_TypeId_Boolean) or (TB = KV_Value_TypeId_Boolean) then
    Result := CompareInt(Ord(A.GetAsBoolean), Ord(B.GetAsBoolean))
  else
  if (TA = KV_Value_TypeId_DateTime) or (TB = KV_Value_TypeId_DateTime) then
    Result := CompareDateTime(A.AsDateTime, B.AsDateTime)
  else
  if (TA = KV_Value_TypeId_String) or (TB = KV_Value_TypeId_String) then
    Result := CompareStr(A.GetAsString, B.GetAsString)
  else
  if (TA = KV_Value_TypeId_Float) or (TB = KV_Value_TypeId_Float) then
    Result := CompareFloat(A.GetAsFloat, B.GetAsFloat)
  else
  if (TA = KV_Value_TypeId_Integer) or (TB = KV_Value_TypeId_Integer) then
    Result := CompareInt(A.GetAsInteger, B.GetAsInteger)
  else
    raise EkvValue.CreateFmt('Type error: Cannot compare types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpIn(const A, B: AkvValue): Boolean;
var
  TA, TB : Byte;
begin
  Assert(Assigned(A));
  Assert(Assigned(B));

  TA := A.TypeId;
  TB := B.TypeId;
  if TB = KV_Value_TypeId_Set then
    Result := TkvSetValue(B).Exists(A.GetAsString)
  else
  if TB in [KV_Value_TypeId_Dictionary, KV_Value_TypeId_Folder] then
    Result := TkvDictionaryValue(B).Exists(A.GetAsString)
  else
  if (TB = KV_Value_TypeId_List) and
     (TA in [KV_Value_TypeId_Integer, KV_Value_TypeId_String, KV_Value_TypeId_Null]) then
    Result := TkvListValue(B).HasValue(A)
  else
    raise EkvValue.CreateFmt('Type error: Cannot apply in operator to types: %s and %s',
        [A.ClassName, B.ClassName]);
end;

function ValueOpAppend(const A, B: AkvValue): AkvValue;
var
  TA, TB : Byte;
  L : TkvListValue;
  D : TkvDictionaryValue;
begin
  Assert(Assigned(A));
  Assert(Assigned(B));

  TA := A.TypeId;
  TB := B.TypeId;
  if TA = KV_Value_TypeId_String then
    Result := TkvStringValue.Create(A.AsString + B.AsString)
  else
  if TA = KV_Value_TypeId_Binary then
    Result := TkvBinaryValue.Create(kvByteArrayAppend(A.GetAsBinary, B.GetAsBinary))
  else
  if TA = KV_Value_TypeId_List then
    begin
      L := TkvListValue(A.Duplicate);
      if TB = KV_Value_TypeId_List then
        L.AddList(TkvListValue(B))
      else
        L.Add(B.Duplicate);
      Result := L;
    end
  else
  if TA in [KV_Value_TypeId_Dictionary, KV_Value_TypeId_Folder] then
    begin
      if not (TB in [KV_Value_TypeId_Dictionary, KV_Value_TypeId_Folder]) then
        raise EkvValue.CreateFmt('Type error: Cannot append types: %s and %s',
           [A.ClassName, B.ClassName]);
      D := TkvDictionaryValue(A.Duplicate);
      D.AddItems(TkvDictionaryValue(B));
      Result := D;
    end
  else
    raise EkvValue.CreateFmt('Type error: Cannot append types: %s and %s',
        [A.ClassName, B.ClassName]);
end;



{ Timestamp encoding }

function kvDateTimeToTimestamp(const D: TDateTime): Int64;
const
  MsPerDay = 24 * 60 * 60 * 1000;
var
  M : Double;
begin
  M := Double(D) * MsPerDay;
  Result := Round(M);
end;

function kvTimestampNow: Int64;
begin
  Result := kvDateTimeToTimestamp(Now);
end;



end.

