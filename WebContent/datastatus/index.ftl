<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>S3P Data Status</title>
</head>
<body>
<table border="1">
<tr>
<th>Job Name</th><th>Last</th><th>Start</th>	
</tr>
<#list statusList as status>
    <tr>
        <td>${status.partitionKey}</td><td>${status.last}</td><td>${status.start}</td>
    </tr>
</#list>
</table>
</body>
</html>