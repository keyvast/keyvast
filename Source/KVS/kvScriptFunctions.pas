{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/03/01  0.01  Initial version }

// Todo: String - LEFT, RIGHT, SUBSTRING, INDEXOF
// Todo: DateTime - DATE(y,m,d) TIME(h,m,s,z)
// Todo: Math - ROUND

{$INCLUDE kvInclude.inc}

unit kvScriptFunctions;

interface

uses
  SysUtils,
  kvValues,
  kvScriptContext;



type
  EkvScriptFunction = class(Exception);

  { Procedure value }

  AkvScriptProcedureValue = class
  public
    function  GetParamCount: Integer; virtual; abstract;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; virtual; abstract;
  end;

  { Length built-in function }

  TkvScriptLengthBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Integer cast built-in function }

  TkvScriptIntegerCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Float cast built-in function }

  TkvScriptFloatCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvFloatValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { String cast built-in function }

  TkvScriptStringCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { DateTime cast built-in function }

  TkvScriptDateTimeCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvDateTimeValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Byte cast built-in function }

  TkvScriptByteCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvBinaryValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Binary cast built-in function }

  TkvScriptBinaryCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvBinaryValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Char cast built-in function }

  TkvScriptCharCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Replace built-in function }

  TkvScriptReplaceBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { GetDate built-in function }

  TkvScriptGetDateBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvDateTimeValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { IsNull built-in function }

  TkvScriptIsNullBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : AkvValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Lower built-in function }

  TkvScriptLowerBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Upper built-in function }

  TkvScriptUpperBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Trim built-in function }

  TkvScriptTrimBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Round built-in function }

  TkvScriptRoundBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  GetParamCount: Integer; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;



implementation

uses
  {$IFDEF MACOS}
  Macapi.CoreFoundation,
  {$ENDIF}
  StrUtils;



{ TkvScriptLengthBuiltInFunction }

destructor TkvScriptLengthBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptLengthBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptLengthBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  L : Integer;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  if V is TkvListValue then
    L := TkvListValue(V).GetCount
  else
  if V is TkvStringValue then
    L := Length(TkvStringValue(V).AsString)
  else
  if V is TkvBinaryValue then
    L := Length(TkvBinaryValue(V).AsBinary)
  else
    raise EkvScriptFunction.Create('Invalid parameter type');
  if not Assigned(FResult) then
    FResult := TkvIntegerValue.Create;
  FResult.AsInteger := L;
  Result := FResult;
end;



{ TkvScriptIntegerCastBuiltInFunction }

destructor TkvScriptIntegerCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptIntegerCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptIntegerCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  L : Int64;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  L := V.AsInteger;
  if not Assigned(FResult) then
    FResult := TkvIntegerValue.Create;
  FResult.AsInteger := L;
  Result := FResult;
end;



{ TkvScriptFloatCastBuiltInFunction }

destructor TkvScriptFloatCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptFloatCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptFloatCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  L : Double;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  L := V.AsFloat;
  if not Assigned(FResult) then
    FResult := TkvFloatValue.Create;
  FResult.AsFloat := L;
  Result := FResult;
end;



{ TkvScriptStringCastBuiltInFunction }

destructor TkvScriptStringCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptStringCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptStringCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  S := V.AsString;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := S;
  Result := FResult;
end;



{ TkvScriptDateTimeCastBuiltInFunction }

destructor TkvScriptDateTimeCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptDateTimeCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptDateTimeCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  D : TDateTime;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  D := V.AsDateTime;
  if not Assigned(FResult) then
    FResult := TkvDateTimeValue.Create;
  FResult.AsDateTime := D;
  Result := FResult;
end;



{ TkvScriptByteCastBuiltInFunction }

destructor TkvScriptByteCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptByteCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptByteCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : Int64;
  A : kvByteArray;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsInteger;
  if B > $FF then
    raise EkvScriptFunction.Create('Invalid byte value');
  if not Assigned(FResult) then
    FResult := TkvBinaryValue.Create;
  SetLength(A, 1);
  A[0] := Byte(B);
  FResult.AsBinary := A;
  Result := FResult;
end;



{ TkvScriptBinaryCastBuiltInFunction }

destructor TkvScriptBinaryCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptBinaryCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptBinaryCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : kvByteArray;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsBinary;
  if not Assigned(FResult) then
    FResult := TkvBinaryValue.Create;
  FResult.AsBinary := B;
  Result := FResult;
end;



{ TkvScriptCharCastBuiltInFunction }

destructor TkvScriptCharCastBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptCharCastBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptCharCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : Int64;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsInteger;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  if B > $FFFF then
    raise EkvScriptFunction.Create('Invalid character value');
  FResult.AsString := WideChar(B);
  Result := FResult;
end;



{ TkvScriptReplaceBuiltInFunction }

destructor TkvScriptReplaceBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptReplaceBuiltInFunction.GetParamCount: Integer;
begin
  Result := 3;
end;

function TkvScriptReplaceBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  VS, VF, VR : AkvValue;
  SS, SF, SR : String;
  S : String;
begin
  if Length(ParamValues) <> 3 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  VS := ParamValues[0];
  VF := ParamValues[1];
  VR := ParamValues[2];
  SS := VS.AsString;
  SF := VF.AsString;
  SR := VR.AsString;
  S := ReplaceText(SS, SF, SR);
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := S;
  Result := FResult;
end;



{ TkvScriptGetDateBuiltInFunction }

destructor TkvScriptGetDateBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptGetDateBuiltInFunction.GetParamCount: Integer;
begin
  Result := 0;
end;

function TkvScriptGetDateBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
begin
  if Length(ParamValues) <> 0 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  if not Assigned(FResult) then
    FResult := TkvDateTimeValue.Create;
  FResult.AsDateTime := Now;
  Result := FResult;
end;



{ TkvScriptIsNullBuiltInFunction }

destructor TkvScriptIsNullBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptIsNullBuiltInFunction.GetParamCount: Integer;
begin
  Result := 2;
end;

function TkvScriptIsNullBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V1 : AkvValue;
  V2 : AkvValue;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  FreeAndNil(FResult);
  V1 := ParamValues[0];
  V2 := ParamValues[1];
  if V1 is TkvNullValue then
    FResult := V2.Duplicate
  else
    FResult := V1.Duplicate;
  Result := FResult;
end;



{ TkvScriptLowerBuiltInFunction }

destructor TkvScriptLowerBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptLowerBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptLowerBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  S := S.ToLower;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := S;
  Result := FResult;
end;



{ TkvScriptUpperBuiltInFunction }

destructor TkvScriptUpperBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptUpperBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptUpperBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  S := S.ToUpper;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := S;
  Result := FResult;
end;



{ TkvScriptTrimBuiltInFunction }

destructor TkvScriptTrimBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptTrimBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptTrimBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  S := S.Trim;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := S;
  Result := FResult;
end;



{ TkvScriptRoundBuiltInFunction }

destructor TkvScriptRoundBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptRoundBuiltInFunction.GetParamCount: Integer;
begin
  Result := 1;
end;

function TkvScriptRoundBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  F : Double;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  F := ParamValues[0].AsFloat;
  if not Assigned(FResult) then
    FResult := TkvIntegerValue.Create;
  FResult.AsInteger := Round(F);
  Result := FResult;
end;



end.

