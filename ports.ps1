$procs = Get-Process

$ports = netstat -ano
$ports[4..$ports.length] |
    # First column comes back empty, so we set to "ProcessName" as a placeholder for later
    ConvertFrom-String -PropertyNames ProcessName,Proto,Local,Remote,State,PID  | 
    where  State -eq 'LISTENING' | 
    foreach {
        $_.ProcessName = ($procs | where ID -eq $_.PID).ProcessName
        $_
    } | 
    Format-Table
