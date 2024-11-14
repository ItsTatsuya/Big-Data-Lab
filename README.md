# Big Data Lab

different output directory name each time or add a timestamp to make it unique:

```
hadoop jar <jarfilename>.jar <functionname> <input>.txt output_$(date +%Y%m%d_%H%M%S)
```