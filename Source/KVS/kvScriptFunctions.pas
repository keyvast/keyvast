{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/03/01  0.01  Initial version }
{ 2018/03/03  0.02  Additional string functions }

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
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; virtual; abstract;
  end;

  { Length built-in function }

  TkvScriptLengthBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Integer cast built-in function }

  TkvScriptIntegerCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Float cast built-in function }

  TkvScriptFloatCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvFloatValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { String cast built-in function }

  TkvScriptStringCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { DateTime cast built-in function }

  TkvScriptDateTimeCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvDateTimeValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Date built-in function }

  TkvScriptDateBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvDateTimeValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Time built-in function }

  TkvScriptTimeBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvDateTimeValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Byte cast built-in function }

  TkvScriptByteCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvBinaryValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Binary cast built-in function }

  TkvScriptBinaryCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvBinaryValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Char cast built-in function }

  TkvScriptCharCastBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Replace built-in function }

  TkvScriptReplaceBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { GetDate built-in function }

  TkvScriptGetDateBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvDateTimeValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { IsNull built-in function }

  TkvScriptIsNullBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : AkvValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Lower built-in function }

  TkvScriptLowerBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Upper built-in function }

  TkvScriptUpperBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Trim built-in function }

  TkvScriptTrimBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Round built-in function }

  TkvScriptRoundBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Substring built-in function }

  TkvScriptSubstringBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { IndexOf built-in function }

  TkvScriptIndexOfBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvIntegerValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Left built-in function }

  TkvScriptLeftBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Right built-in function }

  TkvScriptRightBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvStringValue;
  public
    destructor Destroy; override;
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { SetOf built-in function }

  TkvScriptSetOfBuiltInFunction = class(AkvScriptProcedureValue)
  private
    FResult : TkvSetValue;
  public
    destructor Destroy; override;
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



{ TkvScriptDateBuiltInFunction }

destructor TkvScriptDateBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptDateBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  Ye, Mo, Da : Int64;
  D : TDateTime;
begin
  if Length(ParamValues) <> 3 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  Ye := ParamValues[0].AsInteger;
  Mo := ParamValues[1].AsInteger;
  Da := ParamValues[2].AsInteger;
  D := EncodeDate(Ye, Mo, Da);
  if not Assigned(FResult) then
    FResult := TkvDateTimeValue.Create;
  FResult.AsDateTime := D;
  Result := FResult;
end;



{ TkvScriptTimeBuiltInFunction }

destructor TkvScriptTimeBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptTimeBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  L : Integer;
  Ho, Mi, Se, ZZ : Int64;
  D : TDateTime;
begin
  L := Length(ParamValues);
  if (L < 3) or (L > 4) then
    raise EkvScriptFunction.Create('Invalid parameter count');
  Ho := ParamValues[0].AsInteger;
  Mi := ParamValues[1].AsInteger;
  Se := ParamValues[2].AsInteger;
  if L > 3 then
    ZZ := ParamValues[3].AsInteger
  else
    ZZ := 0;
  D := EncodeTime(Ho, Mi, Se, ZZ);
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



{ TkvScriptSubstringBuiltInFunction }

destructor TkvScriptSubstringBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptSubstringBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
  I, L : Integer;
begin
  if Length(ParamValues) <> 3 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  I := ParamValues[1].AsInteger;
  L := ParamValues[2].AsInteger;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := Copy(S, I + 1, L);
  Result := FResult;
end;



{ TkvScriptIndexOfBuiltInFunction }

destructor TkvScriptIndexOfBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptIndexOfBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S, T : String;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  T := ParamValues[1].AsString;
  if not Assigned(FResult) then
    FResult := TkvIntegerValue.Create;
  FResult.AsInteger := T.IndexOf(S);
  Result := FResult;
end;



{ TkvScriptLeftBuiltInFunction }

destructor TkvScriptLeftBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptLeftBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
  N : Integer;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  N := ParamValues[1].AsInteger;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  FResult.AsString := Copy(S, 1, N);
  Result := FResult;
end;



{ TkvScriptRightBuiltInFunction }

destructor TkvScriptRightBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptRightBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
  N, L : Integer;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  N := ParamValues[1].AsInteger;
  if not Assigned(FResult) then
    FResult := TkvStringValue.Create;
  L := Length(S);
  FResult.AsString := Copy(S, L - N + 1, N);
  Result := FResult;
end;



{ TkvScriptSetOfBuiltInFunction }

destructor TkvScriptSetOfBuiltInFunction.Destroy;
begin
  FreeAndNil(FResult);
  inherited Destroy;
end;

function TkvScriptSetOfBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  L : Integer;
  V : AkvValue;
  Li : TkvListValue;
  I, PaI : Integer;
begin
  L := Length(ParamValues);
  if not Assigned(FResult) then
    FResult := TkvSetValue.Create
  else
    FResult.Clear;
  for PaI := 0 to L - 1 do
    begin
      V := ParamValues[PaI];
      if V is TkvListValue then
        begin
          Li := TkvListValue(V);
          for I := 0 to Li.GetCount - 1 do
            FResult.Add(Li.GetValue(I).AsString);
        end
      else
      if V is TkvStringValue then
        FResult.Add(V.AsString)
      else
        raise EkvScriptFunction.Create('Invalid parameter type: List or string expected');
    end;
  Result := FResult;
end;



end.

