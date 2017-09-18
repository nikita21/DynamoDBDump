    public void exportPaymentSuccessDataToFile() {
    	DynamoAccess dynamoAccess = new DynamoAccess();
    	AmazonDynamoDBClient client = dynamoAccess.getClient();
    	try {
	    	Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
	    	expressionAttributeValues.put(":status1", new AttributeValue().withS("SUCCESS"));
	    	expressionAttributeValues.put(":status2", new AttributeValue().withS("CONFIRMED"));
	    	expressionAttributeValues.put(":status3", new AttributeValue().withS("REFUNDED"));
	    	/*expressionAttributeValues.put(":start", new AttributeValue().withN("1488306600000"));
	    	expressionAttributeValues.put(":end", new AttributeValue().withN("1490985000000"));*/
	    	
	    	Map<String, String> expressionAttributeNames = new HashMap<String, String>();
	    	expressionAttributeNames.put("#date", "timeStamp");

	    	//[timeStamp, amount, Discount, paymentMethod, SessionId, paymentStatus]
	    	Map<String, Integer> columnMap = new HashMap<String, Integer>();
	    	columnMap.put("timeStamp", 1);
	    	columnMap.put("amount", 2);
	    	columnMap.put("Discount", 3);
	    	columnMap.put("paymentMethod", 4);
	    	columnMap.put("SessionId", 5);
	    	columnMap.put("paymentStatus", 6);
	    	saveColumnValues(columnMap);
	    	
	    	ScanResult result = null;
	    	do {
		    	ScanRequest scanRequest = new ScanRequest()
		    		    .withTableName("Payments")
		    		    .withProjectionExpression("SessionId, paymentStatus, paymentMethod, Discount, amount, #date")
		    		    .withExpressionAttributeValues(expressionAttributeValues)
		    		    .withExpressionAttributeNames(expressionAttributeNames)
		    		    .withFilterExpression("paymentStatus IN (:status1, :status2, :status3)")
		    		    .withLimit(50); //AND #date >= :start AND #date <= :end");
		    		   // .withFilterExpression("#date BETWEEN :start AND :end");
		
		    	if(result != null) {
		    		scanRequest.setExclusiveStartKey(result.getLastEvaluatedKey());
		    	}
		    	System.out.println("Scan request complete!!!!");
		    	result = client.scan(scanRequest);
	            
		    	for (Map<String, AttributeValue> item : result.getItems()) {
		    		System.out.println("ITEM: " + item);
		    		parse(item, columnMap);
		    	}
	    	} while (result.getLastEvaluatedKey() != null);
    	} catch (Throwable t) {
    		t.printStackTrace();
    	}
    }
    
    private void parse(Map<String, AttributeValue> item, Map<String, Integer> columnMap)
    {
    	/* @item
    	 * keySet : [timeStamp, amount, Discount, paymentMethod, SessionId, paymentStatus]
		 * values : [{S: 1491983932469,}, {S: 9,}, {N: 4,}, {S: PAYTM,}, {S: e421491983859956,}, {S: SUCCESS,}]
    	 */
    	Map<Integer, String> record = new HashMap<Integer, String>();
        for (String key : item.keySet())
        {
            String keyName = key;
            String type = "";
            if (item.get(key).getS() != null )
            {
                type = "S";
            }
            if (item.get(key).getN() != null)
            {
                type = "N";
            }
            if (item.get(key).getBOOL() != null)
            {
                type = "B";
            }
            //Add as column if it's not a list or map
            if (!columnMap.containsKey(keyName))
            {
                //Add newly discovered column
            	System.out.println("NOT POSSIBLE, ALREADY ADDED COLUMNS");
                columnMap.put(keyName, columnMap.size());
            }

            switch (type)
            {
                case "S":
                    record.put(columnMap.get(keyName), item.get(key).getS());
                    break;
                case "N":
                    record.put(columnMap.get(keyName), item.get(key).getN());
                    break;
                case "B":
                    record.put(columnMap.get(keyName), item.get(key).getBOOL().toString());
                    break;
                default:
                    System.out.println(keyName + " : \t" + item.get(key));
            }
        }
        saveToFile(columnMap, record);
    }
    
    private void saveToFile(Map<String, Integer> columnMap, Map<Integer, String> record) {
    	PrintWriter pw = null;
    	try {
			pw = new PrintWriter(new FileWriter("paymentSuccessData.csv", true));
			StringBuilder sb  = new StringBuilder();
			boolean validDate = false;
			Date d1 = null;
			for(String columnName: columnMap.keySet()) {
				int index = columnMap.get(columnName);
				
				if(record.containsKey(index)) {
					String value = record.get(index);
					if(index == 1) {
						long timestamp = Long.valueOf(value);
						d1 = new Date(timestamp);
						validDate = compareDate(d1);
						sb.append(d1).append(",");
					}
					else if(value != null && !value.isEmpty())
						sb.append(value).append(",");
					else
						sb.append("N/A").append(",");
				} else {
					sb.append("N/A").append(",");
				}
			}
			sb.append("\n");
			System.out.println(sb.toString());
			if(validDate)
				pw.write(sb.toString());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
    	finally {
    		if(pw != null) {
    			pw.close();
    		}
    	}
    }
    
    private void saveColumnValues(Map<String, Integer> columnMap) {
    	PrintWriter pw = null;
    	try {
			pw = new PrintWriter(new FileWriter("paymentSuccessData.csv", true));
			StringBuilder sb = new StringBuilder();
			for(String columnName : columnMap.keySet()) {
				sb.append(columnName).append(",");
			}
			String columnString = sb.toString();
			int len = columnString.length();
			if(columnString.endsWith(","))
				columnString = columnString.substring(0, len-1);
			pw.write(columnString);
			pw.write("\n");
			System.out.println(columnString);
    	} catch (IOException e) {
			e.printStackTrace();
		} finally {
    		if(pw != null) {
    			pw.close();
    		}
    	}
    }
    
    private boolean compareDate(Date date) {
    	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    	String d1 = "2016-04-01";
    	String d2 = "2017-03-31";
    	try {
			Date start = df.parse(d1);
			Date end = df.parse(d2);
			if(date.after(start) && date.before(end))
				return true;
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	return false;
    }
