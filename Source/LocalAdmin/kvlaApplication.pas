{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/11  0.01  Initial development: create, open, delete }

unit kvlaApplication;

interface

uses
  SysUtils,
  kvValues,
  kvDiskSystem,
  kvScriptNodes,
  kvScriptParser,
  kvScriptSystem;



type
  EkvlaApplication = class(Exception);
  TkvlaApplication = class;
  TkvlaApplicationPrintEvent =
      procedure (Sender: TkvlaApplication; const Txt: String) of object;
  TkvlaApplication = class
  private
    FPath    : String;
    FName    : String;
    FOnPrint : TkvlaApplicationPrintEvent;

    FSys     : TkvSystem;
    FMSys    : TkvScriptSystem;
    FSession : TkvScriptSession;
    FParser  : TkvScriptParser;

  protected
    procedure Print(const Txt: String);

  public
    constructor Create;
    destructor Destroy; override;

    property  Path: String read FPath write FPath;
    property  Name: String read FName write FName;
    property  OnPrint: TkvlaApplicationPrintEvent read FOnPrint write FOnPrint;

    procedure CreateSystem;
    procedure DeleteSystem;
    procedure OpenSystem;
    procedure CloseSystem;

    function  GetCommandPrompt: String;
    procedure ExecCommand(const CmdTxt: String);
  end;

var
  App : TkvlaApplication = nil;



implementation



{ TkvlaApplication }

constructor TkvlaApplication.Create;
begin
  inherited Create;
end;

destructor TkvlaApplication.Destroy;
begin
  FreeAndNil(FParser);
  FreeAndNil(FMSys);
  FreeAndNil(FSys);
  inherited Destroy;
end;

procedure TkvlaApplication.Print(const Txt: String);
begin
  if Assigned(FOnPrint) then
    FOnPrint(self, Txt);
end;

procedure TkvlaApplication.CreateSystem;
begin
  Print('Creating system...');
  Print(Path + ' ' + Name);
  FSys := TkvSystem.Create(Path, Name);
  FSys.OpenNew;
  FMSys := TkvScriptSystem.Create(FSys);
  FMSys.OpenNew;
  FSession := FMSys.AddSession;
  FParser := TkvScriptParser.Create;
  Print('System created');
end;

procedure TkvlaApplication.DeleteSystem;
begin
  Print('Deleting system...');
  Print(Path + ' ' + Name);
  FSys := TkvSystem.Create(Path, Name);
  FSys.Open;
  FMSys := TkvScriptSystem.Create(FSys);
  FMSys.Open;
  FSys.Close;
  FMSys.Close;
  FSys.Delete;
  FMSys.Delete;
  Print('System deleted');
end;

procedure TkvlaApplication.OpenSystem;
begin
  Print('Opening system...');
  Print(Path + ' ' + Name);
  FSys := TkvSystem.Create(Path, Name);
  FSys.Open;
  FMSys := TkvScriptSystem.Create(FSys);
  FMSys.Open;
  FSession := FMSys.AddSession;
  FParser := TkvScriptParser.Create;
  Print('System opened');
end;

procedure TkvlaApplication.CloseSystem;
begin
  Print('Closing system...');
  if Assigned(FSession) then
    FSession.Close;
  FSys.Close;
  FMSys.Close;
  Print('System closed');
end;

function TkvlaApplication.GetCommandPrompt: String;
var
  S : String;
  DbN : String;
  DsN : String;
begin
  DbN := FSession.GetSelectedDatabaseName;
  if DbN <> '' then
    begin
      S := DbN + ':';
      DsN := FSession.GetSelectedDatasetName;
      if DsN <> '' then
        S := S + DsN + '\';
    end
  else
    S := '';
  Result := S;
end;

procedure TkvlaApplication.ExecCommand(const CmdTxt: String);
var
  N : AkvScriptNode;
  V : AkvValue;
begin
  N := FParser.Parse(CmdTxt);
  if not Assigned(N) then
    exit;
  if N is AkvScriptStatement then
    begin
      V := AkvScriptStatement(N).Execute(FSession.ScriptContext);
      if Assigned(V) then
        try
          Print(V.AsString);
        finally
          V.Free;
        end;
    end
  else
  if N is AkvScriptExpression then
    begin
      V := AkvScriptExpression(N).Evaluate(FSession.ScriptContext);
      if Assigned(V) then
        try
          Print(V.AsString);
        finally
          V.Free;
        end;
    end
  else
    raise EkvlaApplication.Create('Not executable');
end;



end.

