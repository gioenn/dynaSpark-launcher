SHOW_START_LINES = 0;
SHOW_END_LINES = 1;
SHOW_DEADLINE_LINES = 1;

%Set true (1) only one of the options below for labels placement:
LEGACY_LABEL_PLACEMENT = 0; % Label position as suggested by python tool - Zoomable, Editable
TEXTFIT_LABEL_PLACEMENT = 0; % Automatic placement of adjacent labels - Zoomable, Editable
NOLEGEND_LABEL_PLACEMENT = 0; % Legend-like labels but placement close to lines w/o legend - Zoomable, Editable
ANNOTATION_LABEL_PLACEMENT = 0; % Annotation-like labels with arrows pointing to lines - Editable, not Zoomable
TEXTFIT_ANNOTATION_LABEL_PLACEMENT = 1; % Automatic placement of adjacent labels with annotation-like labels  
                                        % with arrows pointing to lines - Zoomable, Editable only through figure 
                                        % properties text editor (no "drag&drop" manual editing)

Grey=[0.5   0.5   0.5];
CornflowerBlue=[0.3906    0.5820    0.9258];
LightSteelBlue=[0.6875    0.7656    0.8672];
chart=jsondecode(fileread('charts.json'));
for cn = 1:length(chart.Charts)    
    fig=figure;
    hax=axes;
    hold on;
    yyaxis left;
    x1l=chart.Charts(cn).XAxis.B.Limits.Lower;
    x1u=chart.Charts(cn).XAxis.B.Limits.Upper;
    y1l=chart.Charts(cn).YAxis.L.Limits.Lower;
    y1u=chart.Charts(cn).YAxis.L.Limits.Upper;
    set(hax,'XAxisLocation','bottom', 'XLim', [x1l x1u], 'YLim', [y1l y1u]);
    if not (chart.Charts(cn).XAxis.B.TickBase == 0)
        XTick=(x1l:chart.Charts(cn).XAxis.B.TickBase:x1u);
        xticks(XTick);
    end
    YTick=(y1l:chart.Charts(cn).YAxis.L.TickBase:y1u);
    yticks(YTick);
    yyaxis right;
    y2l=chart.Charts(cn).YAxis.R.Limits.Lower;
    y2u=chart.Charts(cn).YAxis.R.Limits.Upper;
    set(hax, 'YLim', [y2l y2u]);
    YTick=(y2l:chart.Charts(cn).YAxis.R.TickBase:y2u);
    yticks(YTick);
    title(hax, {chart.Charts(cn).Name, ''});
    set(hax.XAxis, 'color', 'black');
    xlabel(chart.Charts(cn).XAxis.B.Label);
    set(hax.XAxis, 'TickLabelRotation', 45)
    yyaxis left;
    set(hax.YAxis, 'color', 'black');
    ylabel(chart.Charts(cn).YAxis.L.Label);
    yyaxis right;
    ylabel(chart.Charts(cn).YAxis.R.Label);
    yyaxis left;
    
    %VLines_fields=fieldnames(chart.Charts(cn).VLines);
    %if SHOW_START_LINES
            %VLines_fields=cat(2, VLines_fields, chart.Charts(cn).StartLabels);
    %end
    %if SHOW_END_LINES
        %VLines_fields=cat(2, VLines_fields, chart.Charts(cn).EndLabels);
    %end
    %if SHOW_DEADLINE_LINES & numel(chart.Charts(cn).DeadlineLabels) > 0
        %VLines_fields=cat(2, VLines_fields, chart.Charts(cn).DeadlineLabels);
    %end
    VLines_fields={};
    vls=fieldnames(chart.Charts(cn).VLines);
    for i=1:numel(vls)
        if SHOW_START_LINES & chart.Charts(cn).VLines.(vls{i}).Type=="Start"
            VLines_fields=cat(2, VLines_fields, vls{i});
        end
        if SHOW_END_LINES & chart.Charts(cn).VLines.(vls{i}).Type=="End"
            VLines_fields=cat(2, VLines_fields, vls{i});
        end
        if SHOW_DEADLINE_LINES & chart.Charts(cn).VLines.(vls{i}).Type=="Deadline"
            VLines_fields=cat(2, VLines_fields, vls{i});
        end
    end
    
    %Textfit label placement
    textfit_h={};
    if TEXTFIT_LABEL_PLACEMENT | TEXTFIT_ANNOTATION_LABEL_PLACEMENT
        %Ln=fieldnames(chart.Charts(cn).Labels);
        Ln=VLines_fields;
        Lx=[];
        Ly=[];
        for i=1:numel(Ln)
            Lx(i)=chart.Charts(cn).Labels.(Ln{i}).X;
            %Ly(i)=chart.Charts(cn).Labels.(Ln{i}).Y;
            %Ly(i)=104;
            VLineType=chart.Charts(cn).VLines.(Ln{i}).Type;
            if VLineType=="End"
                Ly(i)=80;
            end
            if VLineType=="Deadline"
                Ly(i)=60;
            end
            if VLineType=="Start"
                Ly(i)=40;
            end 
        end
        %Ln=strcat(Ln, {'\leftarrow'});
        textfit_h=textfit(Lx, Ly, Ln,'FontSize', 10 ,'Rotation', 0, 'HorizontalAlignment', 'center');
        textfit_h_mat=reshape([textfit_h.Position], 3, []);
        
        if TEXTFIT_ANNOTATION_LABEL_PLACEMENT
            for j=1:length(Lx)
                offset_sign = 1;
                if (Lx(j) < 5.0)
                    offset_sign = -1;
                end
                if (abs(textfit_h_mat(1,j) - Lx(j)) < 1.0)
                    offset = 5 * offset_sign;
                else
                    offset=0;
                end
                x=[textfit_h_mat(1,j) - offset Lx(j)];
                %y=[textfit_h_mat(2,j) textfit_h_mat(2,j) - 10];
                y=[Ly(j) textfit_h_mat(2,j) - 10];
                [xg, yg] = ds2nfu(x, y);
                ah=annotation('textarrow',xg ,yg ,'String',Ln(j));
                ah.Parent = gca;  % associate the textarrow annotation to the current axes
                ah.X = x; % the location in data units
                ah.Y = y; % the location in data units
                ah.TextBackgroundColor='y';
                ah.HeadStyle='ellipse';     %'plain'|'fourstar'|'ellipse'|'rectangle'|
                                            %'vback1'|'diamond'|'vback2' (default)|
                                            %'rose'|'vback3'|'hypocycloid'|'cback1'|
                                            %'astroid'|'cback2'|'deltoid'|'cback3'|'none'
                ah.HeadSize= 3;
                uistack(ah,'top');
            end
            delete(textfit_h);
        else
            %quiver(textfit_h_mat(1,:), textfit_h_mat(2,:), Lx - textfit_h_mat(1,:), 90- textfit_h_mat(2,:), 0); %draw arrow to line
        end
    end  
       
    Stages_fields=fieldnames(chart.Charts(cn).Stages);
    for st=1:numel(Stages_fields)
        if (isfield(chart.Charts(cn).Stages.(Stages_fields{st}),'Timestamps'))
            line_progress=plot(chart.Charts(cn).Stages.(Stages_fields{st}).Timestamps, chart.Charts(cn).Stages.(Stages_fields{st}).Progress, 'color', Grey, 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
            line_progreal=plot(chart.Charts(cn).Stages.(Stages_fields{st}).TimestampsReal, chart.Charts(cn).Stages.(Stages_fields{st}).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
        else
            line_progreal=plot(chart.Charts(cn).TimestampsReal, chart.Charts(cn).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
        end
    end
    
    legend_labels=[];
    legend_lines=[];
    for dl=1:numel(VLines_fields)
        cur_field=chart.Charts(cn).VLines.(VLines_fields{dl});
        VLineType=cur_field.Type;
        StartX=cur_field.X;
        EndX=StartX;
        LabelX=StartX;
        Color=cur_field.Color;
        LineStyle=cur_field.Line; 
        Label=VLines_fields{dl};
        LabelY=chart.Charts(cn).Labels.(Label).Y;
        vl=line([StartX EndX], get(hax,'YLim'), 'color', Color, 'LineStyle', LineStyle, 'LineWidth', 2);
        
        %Legacy label placement
        if LEGACY_LABEL_PLACEMENT 
            text(LabelX, LabelY, Label,'FontSize', 12 ,'Rotation', 0, 'HorizontalAlignment', 'center');
        end
        
        %Nolegend labels placement
        legend_lines(dl)=vl; 

        %Annotation labels placement
        if ANNOTATION_LABEL_PLACEMENT
            if (StartX/x1u < 0)
                    sx=0;
                    ex=0;
                else
                    sx=StartX - x1u/50;
                    ex=StartX;
                end
            x=[sx ex];
            if VLineType=="End"
                y=[80 75];
            end
            if VLineType=="Deadline"
                y=[60 55];
            end
            if VLineType=="Start"
                y=[40 35];
            end
            [xf,yf] = ds2nfu(x,y);
            %ya=y*y1u;
            annotation('textarrow',xf ,yf ,'String', Label)
            %ah = annotation('textarrow','String', Label);  % store the textarrow information in ha
            %ah.Parent = gca;           % associate the arrow to the current axes
            %ah.X = x;          % the location in data units
            %ah.Y = ya;         % the location in data units  
            %annotation_pinned('textarrow',xf ,yf ,'String', Label);
            %ah=annotation('textarrow',xf ,yf ,'String', Label);
            % Pin the annotation object to the required axes position
            % Note: some of the following could fail in certain cases - never mind
            %try
                
                %ah.pinAtAffordance(1);
                %ah.pinAtAffordance(2);
                %ah.Pin(1).DataPosition = [sx, y(1), 0];
                %ah.Pin(2).DataPosition = [sx, y(2), 0];
            %catch
                % never mind - ignore (no error)
            %end
            %text(sx, y(1), strcat(Label, {'   '}),'FontSize', 10 ,'Rotation', 0, 'HorizontalAlignment', 'right');
            %text(sx, y(1), strcat({'  '}, '\rightarrow'),'FontSize', 10 ,'Rotation', -45, 'HorizontalAlignment', 'right');
        end
    end
    
    %line_progress=plot(chart.Charts(cn).Timestamps, chart.Charts(cn).Progress, 'color', Grey, 'LineStyle', '-', 'LineWidth', 2);
    %line_progreal=plot(chart.Charts(cn).TimestampsReal, chart.Charts(cn).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2);
    
    yyaxis right;
    line_cpu=plot(chart.Charts(cn).TimestampsCpu, chart.Charts(cn).Cpu, '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);
    xd=get(line_cpu,'xdata');
    yd=get(line_cpu,'ydata');
    a=area(xd, yd);
    set(a, 'FaceAlpha',0.2,'FaceColor', LightSteelBlue);
    
    %Nolegend labels placement
    nolegend_h={};
    if NOLEGEND_LABEL_PLACEMENT
        nolegend_h=nolegend(legend_lines, VLines_fields);
        %uistack(nolegend_h,'top');
    end
end