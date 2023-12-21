import streamlit as st
from PIL import Image
import complete_demo
import datetime

st.title("Intro to Streamlit")
st.caption("APT103 | 11/6/2023")

tab1, tab2, tab3, tab4= st.tabs(["Set Up", "Demo", "Example", "Reference"])

with tab1:
   st.markdown("# Getting Started")
   st.text("Set up requires only a few steps:\n")

   with st.container():
      st.markdown("### Step 1: Download\n")
      st.markdown("#### Run the following code in your vscode terminal:\n")
      
      # if your version of Python is below 3.10 you will need to update Python or downgrade Streamlit.
      # pip install --upgrade streamlit==1.0.0
      st.code("pip install streamlit")
      
   st.write("")
   st.write("")

   with st.container():
      st.markdown("### Step 2: Import the package\n")
      st.markdown("#### Create a .py file and import streamlit as follows:\n")
      st.code("import streamlit as st")

   st.write("")
   st.write("")
   st.divider()

   st.subheader("What is Streamlit?")

   st.markdown("- Free open source framework to build websites")
   st.markdown("- Designed for data science and machnine learning")
   st.markdown("- Allows the creation of machine learning applications")

   st.write("")
   st.write("")

   st.subheader("Why streamlit?")

   st.markdown("- No background in web development required")
   st.markdown("- Uses python (common coding language for data scientists)")
   st.markdown("- Similar to markdown, but dynamic and interactive")
   st.markdown("- Compatible with many data science packages (scikit-learn, keras, PyTorch, latex, numpy, pandas, matplotlib)")
   st.write("")
   st.write("")
   st.write("")
   st.divider()
   
   with st.container():
      st.markdown("### Step 3: Write your streamlit page")
      st.code("""
              import streamlit as st
              
              st.title('Hello Big Data')""")
   st.write("")
   st.write("")

   

      
   with st.container():
      st.markdown("### Step 4: Run your streamlit script in the terminal")
      st.code("streamlit run demo.py")

with tab2:
   complete_demo.load()

with tab3:
   st.header("Text elements and code snippet")
   textExampleCol1, textExampleCol2, = st.columns(2)
   with textExampleCol1:
      ## Title
      ######################################
      st.title("This is Title")
      st.caption("This is Captions")
      st.header("This is header")
      st.subheader("This is subheader")
      st.text("This is text")
      st.write("This is write")
      st.divider()

   with textExampleCol2:
      st.header("Code Snippet")
      textCode = '''
      st.title("This is Title")
      st.caption("This is Captions")
      st.header("This is header")
      st.subheader("This is subheader")
      st.text("This is text")
      st.write("This is write")
      st.divider()

      
      '''
      st.code(textCode, language="python")
      st.divider()

   st.header("input fields")
   inputExampleCol1, inputExampleCol2, = st.columns(2)
   with inputExampleCol1:
      st.subheader("text_input")
      professorName = st.text_input("input  professor name")
            ## Button
      ######################################
      st.subheader("Button")
      submitBtn = st.button("submit")
      cancelBtn = st.button("cancel")
      if submitBtn:
         st.text("This message appears only when   submit button has true state")
         st.text("submit button output :")
         submitBtn
         st.text("cancel button output :")
         cancelBtn

      st.subheader("Select Box")

      with st.form(key='profile_form2'):
         teamName2 = st.text_input("Team Name")
         grade2 = st.selectbox(
      		"Grade",
      		("A","A-","B+","B","B-","other"))
         submitFormBtn = st.form_submit_button ("submit")
         cancelFormBtn = st.form_submit_button ("cancel")
         if submitFormBtn:
            st.text(f"team: {teamName2} got {grade2}  for this presentation!")
      
      st.subheader("Radio Button")
      category = st.radio(
      	"label",
      	("Option1","Option2"))

      st.subheader("Multi Select")
      answers = st.multiselect(
      			"label",(
      		"Option1","Option2","Option3"))

      st.subheader("Check box")
      # Checkbox
      isChecking = st.checkbox("question label :  Ex, do you need notification?")

      st.subheader("Slider")
      # Slider
      range = st.slider("Maximum range",  min_value=80, max_value=220) #how for feet?

      # Date
      st.subheader("Date Picker")
      date = st.date_input(
      		"Choose Target Date",
      		datetime.date(2023,11,1))

      # Color Picker
      st.subheader("Color Picker")
      color = st.color_picker("choose","#ffffff")




   with inputExampleCol2:
      st.header("Code Snippet")
      inputCode = '''
      # TEXT_INPUT
      input_result = st.text_input("inputLable")

      # BUTTON
      if submitBtn:
  	      st.text(f'Hello Bro. {professorName}! Thank   you for giving us a good grade!')
           
      # FORM

        with st.form(key='profile_form'): #declare  form section
         teamName = st.text_input("Team Name")
         grade = st.text_input("Grade")
         submitFormBtn = st.form_submit_button ("submit")
         cancelFormBtn = st.form_submit_button ("cancel")
         if submitFormBtn:
            st.text(f"team:{teamName} got {grade} for   this presentation!")

      ## Select Box
  ######################################
  st.subheader("Select Box")

  with st.form(key='profile_form2'):
  	teamName2 = st.text_input("Team Name")
  	grade2 = st.selectbox(
  		"Grade",
  		("A","A-","B+","B","B-","other"))
  	submitFormBtn = st.form_submit_button ("submit")
  	cancelFormBtn = st.form_submit_button ("cancel")
  	if submitFormBtn:
  		st.text(f"team: {teamName2} got {grade2}  for this presentation!")

  st.divider()

  st.title("Other Input fields Example")

  st.subheader("Radio Button")
  category = st.radio(
  	"label",
  	("Option1","Option2"))

  st.subheader("Multi Select")
  answers = st.multiselect(
  			"label",(
  		"Option1","Option2","Option3"))

  st.subheader("Check box")
  # Checkbox
  isChecking = st.checkbox("question label :  Ex, do you need notification?")

  st.subheader("Slider")
  # Slider
  range = st.slider("Maximum range",  min_value=80, max_value=220) #how for feet?

  # Date
  st.subheader("Date Picker")
  date = st.date_input(
  		"Choose Target Date",
  		datetime.date(2023,11,1))

  # Color Picker
  st.subheader("Color Picker")
  color = st.color_picker("choose","#ffffff")
      '''
      st.code(inputCode, language="python")


with tab4:
   st.header("Reference")
   st.subheader("Official Streamlit Documentations")
   st.link_button("Official Docs","https://docs.streamlit.io/library/api-reference")
   st.subheader("Supporting data frame")
   supportingDataFrameImg = Image.open("supportingDataFrame.png")
   st.image(supportingDataFrameImg, width=1000)
   st.link_button("link to actual page","https://docs.streamlit.io/library/api-reference/data/st.dataframe")

