program KVTestApp;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  flcInteger in '..\Fundamentals\flcInteger.pas',
  flcDecimal in '..\Fundamentals\flcDecimal.pas',
  kvValues in '..\KVS\kvValues.pas',
  kvHash in '..\KVS\kvHash.pas',
  kvHashList in '..\KVS\kvHashList.pas',
  kvStructures in '..\KVS\kvStructures.pas',
  kvFiles in '..\KVS\kvFiles.pas',
  kvObjects in '..\KVS\kvObjects.pas',
  kvScriptContext in '..\KVS\kvScriptContext.pas',
  kvScriptFunctions in '..\KVS\kvScriptFunctions.pas',
  kvScriptNodes in '..\KVS\kvScriptNodes.pas',
  kvScriptParser in '..\KVS\kvScriptParser.pas',
  kvScriptSystem in '..\KVS\kvScriptSystem.pas',
  kvDatasysClient in '..\KVS\kvDatasysClient.pas',
  kvDatasysServer in '..\KVS\kvDatasysServer.pas',
  kvTest in '..\KVS\kvTest.pas';

begin
  try
    flcInteger.Test;
    flcDecimal.Test;
    kvTest.Test;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
