import plotly.plotly as py
import plotly.graph_objs as go
import plotly.offline as offline
import os
import util.utils as utils

def get_scatter(x_axis, res_struct, field1, field2=None):
    # print('get scatter for {}'.format(field1))
    return go.Scatter(
        x=x_axis,
        y=[res_struct[z][field1] + res_struct[z][field2] for z in x_axis] if field2
        else [res_struct[z][field1] for z in x_axis],
        name=field1 + '_+_' + field2 if field2 else field1
    )

def get_scatter2(x_axis, res_struct, field1, field2=None):
    # print('get scatter for {}'.format(field1))
    return go.Scatter(
        x=x_axis,
        y=[res_struct[field1][z] + res_struct[field2][z] for z in x_axis] if field2
        else [res_struct[field1][z] for z in x_axis],
        name=field1 + '_+_' + field2 if field2 else field1
    )


def get_layout(title, x_title, y_title):
    return go.Layout(
        title=title,
        xaxis=dict(
            title=x_title,
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            )
        ),
        yaxis=dict(
            title=y_title,
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            ),
            exponentformat='none'
        )
    )


def plot_figure(data, title, x_axis_label, y_axis_label, out_folder):
    layout = get_layout(title,
                        x_axis_label,
                        y_axis_label)
    fig = go.Figure(data=data, layout=layout)
    # url = py.plot(fig, filename=title, auto_open=False)
    # fig = py.get_figure(url)
    local_path = os.path.abspath(os.path.join(out_folder, '{}.html'.format(fig['layout']['title'])))
    # print("{} -> local: {}".format(url, local_path))
    utils.make_sure_path_exists(os.path.dirname(local_path))
    print("Saving plot in: {}".format(local_path))
    # py.image.save_as(fig, local_path)
    offline.plot(figure_or_data=fig, filename=local_path,
                 # image='png',
                 image_filename=title, auto_open=False)
