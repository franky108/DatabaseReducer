package it.galetta.databaseReducer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class DatabaseReducer {
	private File input;
	private String outputPath;
	private int sizeSplit;
	private String outputExt;
	public DatabaseReducer(String input, String output, String sizeOfSplit, String outputExt) {
		this.input = new File(input);
		this.outputPath = output;
		this.sizeSplit = Integer.valueOf(sizeOfSplit);
		this.outputExt = outputExt;
	}
	public void reduce() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(input));
		Stream<String> lines = reader.lines();
		Iterator<String> linesIt = lines.iterator();
		int linesTaken=0;
		int index = 0;
		File output = new File(outputPath+"split"+index+"."+outputExt);
		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
		if(output.exists())output.delete();
		output.createNewFile();
		while(linesIt.hasNext()) {
			if(linesTaken<=sizeSplit) {
				String line = linesIt.next();
				writer.append(line);
				writer.newLine();
				linesTaken++;
			}
			else {
				writer.close();
				linesTaken=0;
				index++;
				output=new File(outputPath+"split"+index+"."+outputExt);
				if(output.exists())output.delete();
				output.createNewFile();
				writer = new BufferedWriter(new FileWriter(output));
				String line = linesIt.next();
				writer.append(line);
				writer.newLine();
				linesTaken++;
			}

		}
		reader.close();
		writer.close();
		System.out.println("Finito");
	}

	public static void main(String[] args) throws IOException {
		if(args.length<4) {
			System.out.println("Inserire i seguenti parametri:");
			System.out.println("- input \n- output Path \n- numero di elementi da inserire in ogni parte\n- estensione file di output ");
		}
		else {
			System.out.println("Inizio elaborazione");
			DatabaseReducer db = new DatabaseReducer(args[0],args[1],args[2],args[3]);
			db.reduce();

		}

	}

}
