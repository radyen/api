@echo off
:: bhbp procon documents have been uploaded to blob on 11/3/2025 10.30 am 
:: Active blobs: 4,846 blobs, 19.57 GiB (21,017,085,615 bytes).

set app=C:\temp\azcopy.exe
set s3bucket=https://wpl-wrk-cds-np-unzip-bucket.s3.ap-southeast-2.amazonaws.com/


::gcms-hbhp
::procon-hbhp
set blobfolder=prod-procon-hbhp

::CallOff(CO)/
::Framework(FA)/
::Standalone(SA)/
::Master Service Agreement(MSA)/
::keep empty to process all
set common=Standalone(SA)/

::Procon HWEL Load
set aztargetname=HBHP Load

set s3folder=load-to-blob/%blobfolder%/%common%*
set source="%s3bucket%%s3folder%"
set azfolder=%aztargetname%/%common%

::escape % char with another % example: ..Y%#D.. to ..Y%%#D..
set target="https://imtwdegstrprmry.blob.core.windows.net/icmlegacyupload/%azfolder%?si=legacy-manage&spr=https&sv=2022-11-02&sr=c&sig=%%2F....Yw%%2Fo....dY%%..D"
@echo on
%app% copy %source% %target% --recursive

