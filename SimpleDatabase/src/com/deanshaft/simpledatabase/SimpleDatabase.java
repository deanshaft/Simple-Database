package com.deanshaft.simpledatabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Stack;
import java.util.TreeMap;

public class SimpleDatabase {

	/**
	 * This is a TreeMap to represent the key and values of the database.
	 */
	TreeMap<String, String> nameTree;
	/**
	 * This is a TreeMap to represent the number of times a value exists in
	 * the nameTree. If the key does not exist in the valueTree then the value
	 * does not appear in nameTree.
	 */
	TreeMap<String, Integer> valueTree;
	/**
	 * This is a stack that represents the commands that need to be performed
	 * for a rollback to the previous transaction block represented by a null.
	 * If the stack is empty then no transaction block exists. If a transaction
	 * block does not exist then no commands are added to the stack.
	 */
	Stack<String> transactions;
	boolean rollingBack = false; //Determines whether a rollback is performed

	static final String GET_STRING = "GET";
	static final String SET_STRING = "SET";
	static final String UNSET_STRING = "UNSET";
	static final String NUMEQUALTO_STRING = "NUMEQUALTO";
	static final String END_STRING = "END";
	static final String BEGIN_STRING = "BEGIN";
	static final String ROLLBACK_STRING = "ROLLBACK";
	static final String COMMIT_STRING = "COMMIT";
	static final String NULL_STRING = "NULL";
	static final String NO_TRANSACTION_STRING = "NO TRANSACTION";
	static final String ZERO_NUM_VALUE = "0";

	public SimpleDatabase(){
		//Initialize variables
		nameTree = new TreeMap<String, String>();
		valueTree = new TreeMap<String, Integer>();
		transactions = new Stack<String>();
	}

	public void run(){
		BufferedReader br = null;
		String command; //the user input
		try{
			//get commands from user input
			br = new BufferedReader(new InputStreamReader(System.in));
			//continue getting commands while there is input
			while((command = br.readLine()) != null){
				//process the inputed command
				if (!processCommand(command)){
					//if processCommand returns false then END was selected
					break;
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			//close the input stream
			try {
				if (br != null){
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	boolean processCommand(String command){
		String[] commandWords; //all the words in a command
		String firstWordInCommand; //the first word in a command
		//split the commands into words
		commandWords = command.split(" ");
		if (commandWords == null || commandWords.length == 0 ||
				commandWords.length > 3){
			//input error
			printInputErrorMessage();
		} else if (commandWords.length == 1){
			firstWordInCommand = commandWords[0];
			if (firstWordInCommand.equals(END_STRING)){
				//if user inputs END then break out of the while loop
				return false;
			} else if (firstWordInCommand.equals(BEGIN_STRING)){
				//Begin transaction
				performBegin();
			} else if (firstWordInCommand.equals(ROLLBACK_STRING)){
				//Rollback transaction
				performRollback();
			} else if (firstWordInCommand.equals(COMMIT_STRING)){
				//Commit transaction
				performCommit();
			} else{
				//input error
				printInputErrorMessage();
			}
		} else if (commandWords.length == 2){
			firstWordInCommand = commandWords[0];
			if (firstWordInCommand.equals(GET_STRING)){
				//Get command
				performGet(commandWords[1]);
			} else if (firstWordInCommand.equals(UNSET_STRING)) {
				//Unset command
				performUnset(commandWords[1]);
			} else if (firstWordInCommand.equals(NUMEQUALTO_STRING)) {
				//NumEqualTo command
				performNumEqualTo(commandWords[1]);
			} else {
				//input error
				printInputErrorMessage();
			}
		} else if (commandWords.length == 3){
			firstWordInCommand = commandWords[0];
			if (firstWordInCommand.equals(SET_STRING)){
				//Set command
				performSet(commandWords[1], commandWords[2]);
			} else {
				//input error
				printInputErrorMessage();
			}
		}
		return true;
	}

	void printInputErrorMessage(){
		System.out.print("Please enter a valid input. ");
		System.out.println("Note that input is case sensitive.");
	}

	void performGet(String key){
		String value = nameTree.get(key);
		//print value or null if it does not exist
		if (value == null){
			System.out.println(NULL_STRING);
		} else{
			System.out.println(value);
		}
	}

	void performSet(String key, String value){
		String previous = nameTree.put(key, value); //add key to nameTree
		Integer numValue = valueTree.get(value);
		Integer prevNumValue;
		//add value (as key) and numValue (as value) to valueTree
		if (numValue == null){
			numValue = Integer.valueOf(1);
		} else {
			numValue = Integer.valueOf(numValue.intValue() + 1);
		}
		valueTree.put(value, numValue);
		//if previous exists then update valueTree
		if (previous != null){
			prevNumValue = valueTree.get(previous);
			if (prevNumValue.compareTo(Integer.valueOf(1)) > 0){
				prevNumValue = Integer.valueOf(prevNumValue.intValue() - 1);
				valueTree.put(previous, prevNumValue);
			} else {
				valueTree.remove(previous);
			}
		}
		if (!rollingBack){
			updateTransactionsAfterSet(key, previous);
		}
	}

	void performUnset(String key){
		String value = nameTree.remove(key);
		if (value != null){
			//update valueTree
			Integer numValue = valueTree.get(value);
			if (numValue.compareTo(Integer.valueOf(1)) > 0){
				numValue = Integer.valueOf(numValue.intValue() - 1);
				valueTree.put(value, numValue);
			} else {
				valueTree.remove(value);
			}
			if (!rollingBack){
				updateTransactionsAfterUnset(key, value);
			}
		}
	}

	void performNumEqualTo(String value){
		Integer num = valueTree.get(value);
		//print the number having that value
		if (num != null){
			System.out.println(num.toString());
		} else {
			System.out.println(ZERO_NUM_VALUE);
		}
	}

	void performBegin(){
		//new transaction block begins
		transactions.push(null);
	}

	void performRollback(){
		//check if there are transactions
		if (transactions.isEmpty()){
			System.out.println(NO_TRANSACTION_STRING);
		} else {
			//if there are transactions then rollback until this block ends
			rollingBack = true;
			while (transactions.peek() != null){
				String command = transactions.pop();
				processCommand(command);
			}
			transactions.pop();
			rollingBack = false;
		}
	}

	void performCommit(){
		if (transactions.isEmpty()){
			System.out.println(NO_TRANSACTION_STRING);
		} else {
			//a commit clears all transactions in the stack
			transactions.clear();
		}
	}

	void updateTransactionsAfterSet(String key, String value){
		String command;
		//if a block exists
		if (!transactions.isEmpty()){
			if (value == null){
				//previous key did not exist so unset during rollback
				command = createUnsetCommand(key);
			} else {
				//previous key did exist so set previous value during rollback
				command = createSetCommand(key, value);
			}
			transactions.push(command);
		}
	}

	void updateTransactionsAfterUnset(String key, String value){
		//if a block exists
		if (!transactions.isEmpty()){
			//set previous key and value during rollback
			String command = createSetCommand(key, value);
			transactions.push(command);
		}
	}

	String createSetCommand(String key, String value){
		StringBuffer command = new StringBuffer();
		command.append(SET_STRING + " " + key + " " + value);
		return command.toString();
	}

	String createUnsetCommand(String key){
		StringBuffer command = new StringBuffer();
		command.append(UNSET_STRING + " " + key);
		return command.toString();
	}

}
