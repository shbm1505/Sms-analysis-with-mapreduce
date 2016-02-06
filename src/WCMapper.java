import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, Text>
{
      //hadoop supported data types
     
      //map method that performs the tokenizer job and framing the initial key value pairs
      //after all lines are converted into key-value pairs, reducer is called.
      public void map(LongWritable key, Text value, Context context) throws IOException
      {
            //taking one line at a time from input file and tokenizing the same
    	 
    	    
            String line = value.toString();
            String[] sms = line.split("\\|");
      	    String smsText=sms[13];
      	    String smsid=sms[0];
      	    String time=sms[10];
      	    String phoneno=sms[3];
      	   
      	    int priority=0;
      	    String val=" ";
      	    String attribute=" ";
      	    
      	    String st=smsText.toLowerCase();
      	    String[] substring=st.split(" ");
      	    
      	    if(st.startsWith("dear mr.")||st.startsWith("dear sir")||st.startsWith("dear mr.")||st.startsWith("dear salesperson")||st.startsWith("dear mr."))
      	    {
      	    	priority=1;
      	    	val="male";
      	    	attribute="gender";
      	    }
      	    else if(st.startsWith("hi sir")||st.startsWith("hi uncle")||st.startsWith("hi bhaiya")||st.startsWith("hi bro")||st.startsWith("hi brother")||st.startsWith("hi father")||st.startsWith("hi papa")||st.startsWith("hi dad")||st.startsWith("hi pa")||st.startsWith("hi dady")||st.startsWith("hi jiju")||st.startsWith("hi mama")||st.startsWith("hi chacha")||st.startsWith("hi tau")||st.startsWith("hi bhai")||st.startsWith("hi ladke")||st.startsWith("hi mr."))
      	    {
      	    	priority=1;
      	    	val="male";
      	    	attribute="gender";
      	    }
      	    else if(st.startsWith("hi mumma")||st.startsWith("hi mummy")||st.startsWith("hi mom")||st.startsWith("hi di")||st.startsWith("hi sis")||st.startsWith("hi didi")||st.startsWith("hi bua")||st.startsWith("hi bhabhi")||st.startsWith("hi behen ")||st.startsWith("hi behna")||st.startsWith("hi behenji")||st.startsWith("hi aunty")||st.startsWith("hi chachi")||st.startsWith("hi ladki")||st.startsWith("hi mam")||st.startsWith("hi madam")){
      	    	priority=1;
      	    	val="female";
      	    	attribute="gender";
      	    }
      	    else if(st.startsWith("hey sir")||st.startsWith("hey uncle")||st.startsWith("hey bhaiya")||st.startsWith("hey bro")||st.startsWith("hey didi")||st.startsWith("hey brother")||st.startsWith("hey father")||st.startsWith("hey papa")||st.startsWith("hey dad")||st.startsWith("hey pa")||st.startsWith("hi dady")||st.startsWith("hey jiju")||st.startsWith("hey mama")||st.startsWith("hey chacha")||st.startsWith("hey tau")||st.startsWith("hey bhai")||st.startsWith("hey ladke"))
      	    {
      	    	priority=1;
      	    	val="male";
      	    	attribute="gender";
      	    }
      	  else if(st.startsWith("hello sir")||st.startsWith("hello uncle")||st.startsWith("hello bhaiya")||st.startsWith("hello bro")||st.startsWith("hello brother")||st.startsWith("hello father")||st.startsWith("hello papa")||st.startsWith("hello dad")||st.startsWith("hello pa ")||st.startsWith("hello dady")||st.startsWith("hello jiju")||st.startsWith("hello mama")||st.startsWith("hello chacha")||st.startsWith("hello tau")||st.startsWith("hello bhai")||st.startsWith("hello ladke"))
    	    {
    	    	priority=1;
    	    	val="male";
    	    	attribute="gender";
    	    }
      	
      	else if(st.startsWith("hey mumma")||st.startsWith("hey mummy")||st.startsWith("hey mom")||st.startsWith("hey di")||st.startsWith("hey sis")||st.startsWith("hey didi")||st.startsWith("hey bua")||st.startsWith("hey bhabhi")||st.startsWith("hey behen ")||st.startsWith("hey behna")||st.startsWith("hey behenji")||st.startsWith("hey aunty")||st.startsWith("hey chachi")||st.startsWith("hey ladki")||st.startsWith("hey mam")||st.startsWith("hey madam")||st.startsWith("hey madam")){
  	    	priority=1;
  	    	val="female";
  	    	attribute="gender";
  	    }
      	else if(st.startsWith("dear mrs.")||st.startsWith("dear mam")||st.startsWith("dear madam")||st.startsWith("dear ms."))
    	    {
    	    	priority=1;
    	    	val="female";
    	    	attribute="gender";
    	    }
      	else if(st.startsWith("aur mote")||st.startsWith("aur londe"))
 	    {
 	    	priority=1;
 	    	val="male";
 	    	attribute="gender";
 	    }
      	else if(st.startsWith("aur moti"))
 	    {
 	    	priority=1;
 	    	val="female";
 	    	attribute="gender";
 	    }
      	else if((st.startsWith("dear")||st.startsWith("hi")||st.startsWith("hello")||st.startsWith("hey"))&&(substring[1]!="customer")&&(substring[1]!="executive")&&(substring[1]!="student")){
      		
      	int	l=substring[1].length();
      	if(substring[1].charAt(l-1)=='a'||substring[1].charAt(l-1)=='e'||substring[1].charAt(l-1)=='i'||substring[1].charAt(l-1)=='o'||substring[1].charAt(l-1)=='u')
      		val="female";
      	else
      		val="male";
      	attribute="gender";
      	priority=2;
      	    	    }
      	    
      	    
     
  		try {
			context.write(new Text(smsid), new Text(phoneno+"\t"+time+"\t"+attribute+"\t"+val+"\t"+priority));
  	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 
    
            
       }
}