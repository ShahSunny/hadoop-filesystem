package org.sunny;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App {
	

public static void main(String[] args) throws Exception {
	int exitcode = ToolRunner.run(new AppRunner(), args);
	System.exit(exitcode);
}

public static class AppRunner extends Configured implements Tool {
	public AppRunner() {
		super();
	}
	
	public int run(String[] args)  throws Exception {
		Configuration conf = getConf();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		setConf(conf);
        Path[] paths = filterOutPaths(args[0]);
        printPaths(paths);
		return 0;
	}


	private void printPaths(Path[] paths) {
		for (Path p : paths) {
			System.out.println(p.getName());
		}		
	}

	private Path[] filterOutPaths(String path) {
		try {
			FileSystem fs = FileSystem.get(getConf());
			FileStatus[] filesFound= fs.listStatus(new Path(path), new PathFilter() {
				public boolean accept(Path path) {
					FileStatus fileStatus = fs.getFileStatus(path);
					if(fileStatus.isDirectory() && fileStatus.getName().matches("^190.$")) {
						return true;
					} else {
						return false;
					}
				}
			});
			return FileUtil.stat2Paths(filesFound);			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}
}

}