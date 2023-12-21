def load():
    import polars as pl
    import plotly.express as px
    import pyarrow.parquet as pq

    # STEP 1) Import and Text
    import streamlit as st
    st.title('Estimated Church Activity by Tract')
    st.caption("APT103 | 11/6/2023")



    # STEP 2) Display Dataframe
    dat = pl.from_arrow(pq.read_table("target.parquet"))
    dat = dat.with_columns(pl.col("tractcode").cast(pl.Utf8))

    st.header("Raw Target Data")
    st.dataframe(   #https://docs.streamlit.io/library/api-reference/data/st.dataframe
        data = dat,
        hide_index = True,
        height = 200, # width and height in pixels
        column_order = dat.columns[:-1] #remove county
    )



    # STEP 3) Show the code for percent_active
    dat = dat.with_columns(
        percent_active = (
            pl.when(pl.col("population") == 0)
                .then(0)
                .otherwise(pl.col("target") / pl.col("population") * 100)
        )
    )

    st.header("Adding Percent Active")
    st.code("""
        dat = dat.with_columns(
            percent_active = (
                pl.when(pl.col("population") == 0)
                    .then(0)
                    .otherwise(pl.col("target") / pl.col("population") * 100)
            )
        )
    """)


    # STEP 4) Filter the Dataframe
    with st.container():
        rexburg_tracts = ["16065950100", "16065950200", "16065950400", "16065950301", "16065950500", "16065950302"]
        courd_tracts = ["16055000402", "16055000401", "16055001200", "16055000900"]

        dataCol1, dataCol2 = st.columns([1,2])


        with dataCol1:
            st.markdown("### Filter")
            options = ["Idaho", "Rexburg", "Coeur d'Alene"]
            selection = st.selectbox("Select a Geographic Region", options)

        with dataCol2:
            st.subheader(f'{selection} Tracts')
            if selection == options[1]:
                filtered = dat.filter(pl.col("tractcode").is_in(rexburg_tracts))
            elif selection == options[2]:
                filtered = dat.filter(pl.col("tractcode").is_in(courd_tracts))
            else:
                filtered = dat

            st.dataframe(data=filtered, hide_index=True)



    # STEP 5) Describe the Dataframe
    with st.container():
        description = filtered.select("target").describe()
        mean = round(description.select("target")[2].item(), 2)
        std = round(description.select("target")[3].item(), 2)
        min = round(description.select("target")[4].item(), 2)
        max = round(description.select("target")[8].item(), 2)


        st.subheader("Metrics")

        metCol1,metCol2,metCol3,metCol4, = st.columns(4)

        st.dataframe(data=description.transpose(include_header=False, column_names="describe"), hide_index=True)
        with metCol1:
            st.metric(label="mean", value=mean, delta="13")
        with metCol2:
            st.metric(label="std", value=std, delta="0", delta_color="off")
        with metCol3:
            st.metric(label="min", value=min, delta="-100")
        with metCol4:
            st.metric(label="max", value=max, delta="-1", delta_color="inverse")




    # STEP 6) Show Histogram Charts
    fig1 = px.histogram(filtered.sort(by=pl.col("target"), descending=True).head(10), 
                        x="tractcode", 
                        y="target",
                        color_discrete_sequence=['#EA6633'])
    fig1.update_layout(title=f"Count of Active Members per {selection} Tract",
                       xaxis_type="category",
                       bargap=0.2,
                       xaxis_title="Tract Code",
                       yaxis_title="Active Members")

    fig2 = px.histogram(filtered.sort(by=pl.col("percent_active"), descending=True).head(10), 
                        x="tractcode", 
                        y="percent_active",
                        color_discrete_sequence=['#33B6EA'])
    fig2.update_layout(title=f"Percent of Active Members per {selection} Tract",
                       xaxis_type="category",
                       bargap=0.2,
                       xaxis_title="Tract Code",
                       yaxis_title="Percentage of Active LDS Members")
    fig2.update_yaxes(range=[0, 100])


    chartCol1, chartCol2 = st.columns(2)
    with chartCol1:
        st.plotly_chart(fig1, use_container_width=400)
    with chartCol2:
        st.plotly_chart(fig2, use_container_width=400)

if __name__ == "__main__":
    load()