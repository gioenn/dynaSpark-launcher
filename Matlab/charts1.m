function charts
SHOW_START_LINES = 0;
SHOW_END_LINES = 1;
SHOW_DEADLINE_LINES = 1;

%Set true (1) only one of the options below for labels placement:
LEGACY_LABEL_PLACEMENT = 0; % Label position as suggested by python tool - Zoomable, Editable
TEXTFIT_LABEL_PLACEMENT = 0; % Automatic placement of adjacent labels - Zoomable, Editable
NOLEGEND_LABEL_PLACEMENT = 0; % Legend-like labels but placement close to lines w/o legend - Zoomable, Editable
ANNOTATION_LABEL_PLACEMENT = 0; % Annotation-like labels with arrows pointing to lines - Editable, not Zoomable
TEXTFIT_ANNOTATION_LABEL_PLACEMENT = 1; % Automatic placement of adjacent labels with annotation-like labels
                                        % with arrows pointing to lines. Zoomable, Editable only through figure 
                                        % properties text editor (no "drag&drop" manual editing), 
                                        % and aided interactive click-try-&-chose label placement loop, 
                                        % keyboard 's' to skip current label, 'n' to skip current figure, 
                                        % 'return' to confirm last mouse click 'try' placement
DAVINCI_ARROWHEADS =0;
ZOOMABLE_TEXT_LABELS = 0;
ZOOM_CONTROL = 1;

Grey=[0.5   0.5   0.5];
CornflowerBlue=[0.3906    0.5820    0.9258];
LightSteelBlue=[0.6875    0.7656    0.8672];
chart=jsondecode(fileread('charts.json'));
%persistent ahv;
persistent akv;
for cn = 1:length(chart.Charts)   
    fig=figure;
    [hax, h1, h2] = plotyy([0],[0],[0],[0], 'plot');
    set(hax(1),'Box','off')
    set(gca,'color','none');
    delete(h1);
    delete(h2);
    axes(hax(1));
    hold on;
    x1l=chart.Charts(cn).XAxis.B.Limits.Lower;
    x1u=chart.Charts(cn).XAxis.B.Limits.Upper;
    y1l=chart.Charts(cn).YAxis.L.Limits.Lower;
    y1u=chart.Charts(cn).YAxis.L.Limits.Upper;
    set(hax(1),'XAxisLocation','bottom', 'XLim', [x1l x1u], 'YLim', [y1l y1u]);
    if not (chart.Charts(cn).XAxis.B.TickBase == 0)
        XTick=(x1l:chart.Charts(cn).XAxis.B.TickBase:x1u);
        xticks(XTick);
    end
    YTick=(y1l:chart.Charts(cn).YAxis.L.TickBase:y1u);
    yticks(YTick);
    axes(hax(2));
    hold on;
    y2l=chart.Charts(cn).YAxis.R.Limits.Lower;
    y2u=chart.Charts(cn).YAxis.R.Limits.Upper;
    set(hax(2),'XAxisLocation','bottom', 'XLim', [x1l x1u], 'YLim', [y2l y2u]);
    YTick=(y2l:chart.Charts(cn).YAxis.R.TickBase:y2u);
    yticks(YTick);
    title(hax(2), {chart.Charts(cn).Name, ''});
    set(hax(2).XAxis, 'color', 'black');
    set(hax(2).YAxis, 'color', 'black');
    xlabel(chart.Charts(cn).XAxis.B.Label);
    set(hax(1).XAxis, 'TickLabelRotation', 45)
    axes(hax(1));
    hold on;
    set(hax(1).YAxis, 'color', 'black');
    ylabel(chart.Charts(cn).YAxis.L.Label);
    axes(hax(2));
    hold on;
    ylabel(chart.Charts(cn).YAxis.R.Label);
    axes(hax(1));
    hold on;
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
      
    no_stage_plot=1;
    Stages_fields=fieldnames(chart.Charts(cn).Stages);
    for st=1:numel(Stages_fields)
        if (isfield(chart.Charts(cn).Stages.(Stages_fields{st}),'Timestamps'))
            line_progress=plot(chart.Charts(cn).Stages.(Stages_fields{st}).Timestamps, chart.Charts(cn).Stages.(Stages_fields{st}).Progress, 'color', Grey, 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
            line_progreal=plot(chart.Charts(cn).Stages.(Stages_fields{st}).TimestampsReal, chart.Charts(cn).Stages.(Stages_fields{st}).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
        end    
        if (isfield(chart.Charts(cn).Stages.(Stages_fields{st}),'StartTimestamp'))
            no_stage_plot=0;
        end
    end
    if no_stage_plot
       line_progreal=plot(chart.Charts(cn).TimestampsReal, chart.Charts(cn).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
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
        vl=line(hax(1),[StartX EndX], get(hax(1),'YLim'), 'color', Color, 'LineStyle', LineStyle, 'LineWidth', 2);
        
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
            [xf,yf] = ds2nfu(hax(1),x,y);
            annotation('textarrow',xf ,yf ,'String', Label);
        end
    end 
    
    %%plot cpu line
    axes(hax(2));
    hold on;
    set(gca,'color','w');
    line_cpu=plot(hax(2),chart.Charts(cn).TimestampsCpu, chart.Charts(cn).Cpu, '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);
    xd=get(line_cpu,'xdata');
    yd=get(line_cpu,'ydata');
    a=area(xd, yd);
    set(a, 'FaceAlpha',0.2,'FaceColor', LightSteelBlue);
    uistack(a, 'top');
    
    %Textfit label placement
    axes(hax(1));
    hold on;
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
        textfit_h=textfit(Lx, Ly, Ln,'FontSize', 10 ,'Rotation', 0, 'HorizontalAlignment', 'center');
        textfit_h_mat=reshape([textfit_h.Position], 3, []);
        
        if TEXTFIT_ANNOTATION_LABEL_PLACEMENT
            delete(textfit_h);
            ahv=[];
            akv=[];
            ch='';
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
                [xg, yg] = ds2nfu(hax(1), x, y);
                ahv(j)=annotation('textarrow',xg ,yg ,'String',Ln(j));
                set(ahv(j),'TextBackgroundColor','w');
                set(ahv(j),'HeadStyle','ellipse');%'plain'|'fourstar'|'ellipse'|'rectangle'|
                                                  %'vback1'|'diamond'|'vback2' (default)|
                                                  %'rose'|'vback3'|'hypocycloid'|'cback1'|
                                                  %'astroid'|'cback2'|'deltoid'|'cback3'|'none'
                set(ahv(j),'HeadSize',3);
                uistack(ahv(j),'top');
                %% Following code moved after label placing loop: re-enable to place labels from scratch
                %k=0;
                %while k==0
                %    cur_label=Ln(j);
                %    set(gcf, 'WindowButtonMotionFcn', @mouseMove);
                %    k=waitforbuttonpress;
                %end
                %C=get(gca, 'CurrentPoint');
                %x(1)=C(1,1);
                %y(1)=C(1,2);
                %ahv(j).Parent = hax(1);  % associate the textarrow annotation to the current axes;
                set(ahv(j), 'Parent', hax(1));
                set(ahv(j),'X',x);
                set(ahv(j),'Y',y);
            end
            ch='';
            for j=1:length(ahv)
                x=get(ahv(j),'X');
                y=get(ahv(j),'Y');
                cur_label=get(ahv(j),'String');
                [xg, yg] = ds2nfu(hax(1), x, y);
                delete(ahv(j));
                ahv(j)=annotation('textarrow',xg ,yg ,'String', cur_label);
                set(ahv(j),'TextBackgroundColor','m');
                %set(ah,'HeadStyle','ellipse');
                %set(ah,'HeadSize', 3);
                uistack(ahv(j),'top');
                ox=x;
                oy=y;
                if ch=='s' | ch=='S'
                   ch='';
                end
                if  ch=='n' | ch=='N'
                    k=1;
                else
                    k=0;
                end
                while k==0
                    set(gcf, 'WindowButtonMotionFcn', @mouseMove);
                    k=waitforbuttonpress;
                    C=get(gca, 'CurrentPoint');
                    if not(k==0)
                        ch=get(gcf, 'CurrentCharacter');
                    end
                    if ch=='s' | ch=='S' | ch=='n' | ch=='N'
                        k=1;
                        x=ox;
                        y=oy;
                    else 
                        x(1)=C(1,1);
                        y(1)=C(1,2);
                    
                        [xg, yg] = ds2nfu(hax(1), x, y);
                        akv=[akv annotation('textarrow',xg ,yg ,'String', cur_label)];
                        set(akv(end),'TextBackgroundColor','c');
                        %set(akv(end),'HeadStyle','ellipse');
                        %set(akv(end),'HeadSize', 3);
                    end
                end
                C=get(gca, 'CurrentPoint');
                %x(1)=C(1,1);
                %y(1)=C(1,2);
                delete(ahv(j));
                delete(akv);
                [xg, yg] = ds2nfu(hax(1), x, y);
                ahv(j)=annotation('textarrow',xg ,yg ,'String', cur_label);
                set(ahv(j),'TextBackgroundColor','y');
                set(ahv(j),'Parent', hax(1));  % associate the textarrow annotation to the current axes;
                %[x, y] = nfu2ds(hax(1), xg, yg);
                set(ahv(j),'X',x); % the location in data units
                set(ahv(j),'Y',y); % the location in data units
                %axes('position', [200, 10, 205, 12]);
                %axes('position', [x(1), y(1), x(2), y(2)]);                
                %imshow(im);
                %set(ah,'HeadStyle','ellipse');
                set(ahv(j),'HeadStyle','none');
                set(ahv(j),'HeadSize', 3);
                tag = char(strcat('chart', int2str(cn)));
                set(ahv(j),'Tag', tag);
                if DAVINCI_ARROWHEADS
                    set(ahv(j),'HeadStyle','none');
                    %% quiver does not fit very well the purpose as arrow head size is proportional to length of arrow
                    %q=quiver(x(1), y(1), x(2)-x(1), y(2)-y(1), 0); %draw arrow to line
                    %q.Color=[0 0 0];
                    AH_MAX=((x(1)-x(2))^2 + (y(1)-y(2))^2)^(1/2);
                    ahl=2.0;
                    ahw=2.0;
                    if (ahl > AH_MAX)
                        ahl=AH_MAX;
                        ahw=ahw/ahl*AH_MAX;
                    end
                    %% DaVinci is much better - license to use arrows is free
                    davinci( 'arrow', 'X', x, 'Y', y, 'Head.Length', ahl, 'Head.Width', ahw);    % Draw an arrow.
                    %axis(axis);
                    %ar=arrow([x(1), y(1)],[x(2),y(2)]);    % Draw an arrow.
                    uistack(ah,'top');
                    %uistack(ar,'top');
                end
                if ZOOMABLE_TEXT_LABELS
                    im = image(str2im(ah.String, [1 0 0 5], 'back', 'y'));
                    im.XData=[x(1)-10 x(1)];
                    im.YData=[y(1)+3.0 y(1)];
                    delete(ahv(j));
                end
            end                   
            %delete(textfit_h);
        else
            %q=quiver(textfit_h_mat(1,:), textfit_h_mat(2,:), Lx - textfit_h_mat(1,:), 90- textfit_h_mat(2,:), 0); %draw arrow to line
            %q.Color=[0 0 0];
        end
    title(gca,'');    
    end
    
    %Nolegend labels placement
    nolegend_h={};
    if NOLEGEND_LABEL_PLACEMENT
        nolegend_h=nolegend(legend_lines, VLines_fields);
        %uistack(nolegend_h,'top');
    end
    
    %%re-plot cpu line
    axes(hax(1));
    set(gca,'color','w');
    delete(a);
    axes(hax(2));
    hold on;
    set(gca,'color','none');
    line_cpu=plot(hax(2),chart.Charts(cn).TimestampsCpu, chart.Charts(cn).Cpu, '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);
    xd=get(line_cpu,'xdata');
    yd=get(line_cpu,'ydata');
    a=area(xd, yd);
    set(a, 'FaceAlpha',0.2,'FaceColor', LightSteelBlue);
    %uistack(a, 'top');
    %axes(hax(1));
    if ZOOM_CONTROL
        h = zoom;
        %h.Motion = 'horizontal';
        h.ActionPreCallback = @myprecallback;
        h.ActionPostCallback = @mypostcallback;
        h.Enable = 'on';
    end
    %delete(ahv);
    origLim = [hax(1).XLim hax(1).YLim];
    origZoomLevel = 1.0;
    s.o = origLim;
    s.d = textfit_h_mat;
    s.lx = Lx;
    s.ly = Ly;
    s.ln = Ln;
    s.cn = cn;
    fig.UserData = s;
    %msgbox(sprintf('The original X-Limits are [%.2f %.2f].', origLim));
    zoom(1);
end

function myprecallback(obj,evd)
    disp('A zoom is about to occur.');
    origLim = obj.UserData.o;
    cn = obj.UserData.cn;
    %delete(ahv);
    tag = char(strcat('chart', int2str(cn)));
    delete(findall(gcf,'Tag', tag));
    %msgbox(sprintf('Pre: The original X and Y Limits are [%.2f %.2f %.2f %.2f].', origLim));
    %msgbox(sprintf('[%.2f %.2f %.2f].', data));
end

function mypostcallback(obj,evd)
    newLim = [evd.Axes.XLim evd.Axes.YLim];
    origLim = obj.UserData.o;
    axes = evd.Axes(1);
    Lx = obj.UserData.lx;
    Ly = obj.UserData.ly;
    Ln = obj.UserData.ln;
    newRangeX = newLim(2) - newLim(1);
    newRangeY = newLim(6) - newLim(5);
    ZoomLevels = [((origLim(2) - origLim(1)) / (newLim(2) - newLim(1))) ((origLim(4) - origLim(3)) / (newLim(6) - newLim(5)))];
    ZX = ZoomLevels(1);
    ZY = ZoomLevels(2);
    %msgbox(sprintf('Post: The original X and Y Limits are [%.2f %.2f %.2f %.2f].', origLim));
    %msgbox(sprintf('Post: The new X and Y Limits are [%.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f].', newLim));
    %msgbox(sprintf('The new X-ZoomLevel is %.2f, Y-ZoomLevel is %.2f', ZoomLevels));
    %textfit_h_mat = obj.UserData.d;
    textfit_h=textfit(Lx, Ly, Ln,'FontSize', 10 ,'Rotation', 0, 'HorizontalAlignment', 'center');
    textfit_h_mat=reshape([textfit_h.Position], 3, []);
    delete(textfit_h);
    ahv=[];
    akv=[];
    for j=1:length(Lx)
        offset_sign = 1;
        if (Lx(j) < 5.0/ZX)
            offset_sign = -1;
        end
        if (abs(textfit_h_mat(1,j) - Lx(j)) < 1.0/ZX)
            offset = 5/ZX * offset_sign;
        else
            offset=0;
        end
        x=[textfit_h_mat(1,j) - offset Lx(j)];
        %y=[textfit_h_mat(2,j) textfit_h_mat(2,j) - 10];
        y=[Ly(j)/100*newRangeY + newLim(5) Ly(j)/100*newRangeY + newLim(5) - 10/ZY];
        
        if ( x(1) > newLim(1) & x(1) < newLim(2) )%& y(2) > newLim(5) & y(2) < newLim(6) )
            %msgbox(sprintf('x is %.2f %.2f, y is %.2f %.2f', [x y]));
            [xg, yg] = ds2nfu(evd.Axes(1), x, y);
            %msgbox(sprintf('xg is %.2f %.2f, yg is %.2f %.2f', [xg, yg]));
            ahv(j)=annotation('textarrow',xg ,yg ,'String',Ln(j));
            bg_color = 'w';
            lb = char(Ln(j));
            switch lb(1:1)
                case 'S'
                    bg_color = 'b';
                case 'E'
                    bg_color = 'g';
                case 'D'
                    bg_color = 'r';
                otherwise
                    bg_color = 'y';
            end
            set(ahv(j),'TextBackgroundColor',bg_color);
            set(ahv(j),'HeadStyle','vback2');
            set(ahv(j),'HeadSize',6);
            tag = char(strcat('chart', int2str(cn)));
            set(ahv(j),'Tag', tag);
            uistack(ahv(j),'top');
        end
    end
end

end
function chart()
   
    fig=figure;
    [hax, h1, h2] = plotyy([0],[0],[0],[0], 'plot');
    set(hax(1),'Box','off')
    set(gca,'color','none');
    delete(h1);
    delete(h2);
    axes(hax(1));
    hold on;
    x1l=chart.Charts(cn).XAxis.B.Limits.Lower;
    x1u=chart.Charts(cn).XAxis.B.Limits.Upper;
    y1l=chart.Charts(cn).YAxis.L.Limits.Lower;
    y1u=chart.Charts(cn).YAxis.L.Limits.Upper;
    set(hax(1),'XAxisLocation','bottom', 'XLim', [x1l x1u], 'YLim', [y1l y1u]);
    if not (chart.Charts(cn).XAxis.B.TickBase == 0)
        XTick=(x1l:chart.Charts(cn).XAxis.B.TickBase:x1u);
        xticks(XTick);
    end
    YTick=(y1l:chart.Charts(cn).YAxis.L.TickBase:y1u);
    yticks(YTick);
    axes(hax(2));
    hold on;
    y2l=chart.Charts(cn).YAxis.R.Limits.Lower;
    y2u=chart.Charts(cn).YAxis.R.Limits.Upper;
    set(hax(2),'XAxisLocation','bottom', 'XLim', [x1l x1u], 'YLim', [y2l y2u]);
    YTick=(y2l:chart.Charts(cn).YAxis.R.TickBase:y2u);
    yticks(YTick);
    title(hax(2), {chart.Charts(cn).Name, ''});
    set(hax(2).XAxis, 'color', 'black');
    set(hax(2).YAxis, 'color', 'black');
    xlabel(chart.Charts(cn).XAxis.B.Label);
    set(hax(1).XAxis, 'TickLabelRotation', 45)
    axes(hax(1));
    hold on;
    set(hax(1).YAxis, 'color', 'black');
    ylabel(chart.Charts(cn).YAxis.L.Label);
    axes(hax(2));
    hold on;
    ylabel(chart.Charts(cn).YAxis.R.Label);
    axes(hax(1));
    hold on;
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
      
    no_stage_plot=1;
    Stages_fields=fieldnames(chart.Charts(cn).Stages);
    for st=1:numel(Stages_fields)
        if (isfield(chart.Charts(cn).Stages.(Stages_fields{st}),'Timestamps'))
            line_progress=plot(chart.Charts(cn).Stages.(Stages_fields{st}).Timestamps, chart.Charts(cn).Stages.(Stages_fields{st}).Progress, 'color', Grey, 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
            line_progreal=plot(chart.Charts(cn).Stages.(Stages_fields{st}).TimestampsReal, chart.Charts(cn).Stages.(Stages_fields{st}).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
        end    
        if (isfield(chart.Charts(cn).Stages.(Stages_fields{st}),'StartTimestamp'))
            no_stage_plot=0;
        end
    end
    if no_stage_plot
       line_progreal=plot(chart.Charts(cn).TimestampsReal, chart.Charts(cn).ProgressReal, 'color', 'black', 'LineStyle', '-', 'LineWidth', 2, 'Marker', 'none');
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
        vl=line(hax(1),[StartX EndX], get(hax(1),'YLim'), 'color', Color, 'LineStyle', LineStyle, 'LineWidth', 2);
        
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
            [xf,yf] = ds2nfu(hax(1),x,y);
            annotation('textarrow',xf ,yf ,'String', Label);
        end
    end 
    
    %%plot cpu line
    axes(hax(2));
    hold on;
    set(gca,'color','w');
    line_cpu=plot(hax(2),chart.Charts(cn).TimestampsCpu, chart.Charts(cn).Cpu, '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);
    xd=get(line_cpu,'xdata');
    yd=get(line_cpu,'ydata');
    a=area(xd, yd);
    set(a, 'FaceAlpha',0.2,'FaceColor', LightSteelBlue);
    uistack(a, 'top');
    
    %Textfit label placement
    axes(hax(1));
    hold on;
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
        textfit_h=textfit(Lx, Ly, Ln,'FontSize', 10 ,'Rotation', 0, 'HorizontalAlignment', 'center');
        textfit_h_mat=reshape([textfit_h.Position], 3, []);
        
        if TEXTFIT_ANNOTATION_LABEL_PLACEMENT
            delete(textfit_h);
            ahv=[];
            akv=[];
            ch='';
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
                [xg, yg] = ds2nfu(hax(1), x, y);
                ahv(j)=annotation('textarrow',xg ,yg ,'String',Ln(j));
                set(ahv(j),'TextBackgroundColor','w');
                set(ahv(j),'HeadStyle','ellipse');%'plain'|'fourstar'|'ellipse'|'rectangle'|
                                                  %'vback1'|'diamond'|'vback2' (default)|
                                                  %'rose'|'vback3'|'hypocycloid'|'cback1'|
                                                  %'astroid'|'cback2'|'deltoid'|'cback3'|'none'
                set(ahv(j),'HeadSize',3);
                uistack(ahv(j),'top');
                %% Following code moved after label placing loop: re-enable to place labels from scratch
                %k=0;
                %while k==0
                %    cur_label=Ln(j);
                %    set(gcf, 'WindowButtonMotionFcn', @mouseMove);
                %    k=waitforbuttonpress;
                %end
                %C=get(gca, 'CurrentPoint');
                %x(1)=C(1,1);
                %y(1)=C(1,2);
                %ahv(j).Parent = hax(1);  % associate the textarrow annotation to the current axes;
                set(ahv(j), 'Parent', hax(1));
                set(ahv(j),'X',x);
                set(ahv(j),'Y',y);
            end
            ch='';
            for j=1:length(ahv)
                x=get(ahv(j),'X');
                y=get(ahv(j),'Y');
                cur_label=get(ahv(j),'String');
                [xg, yg] = ds2nfu(hax(1), x, y);
                delete(ahv(j));
                ahv(j)=annotation('textarrow',xg ,yg ,'String', cur_label);
                set(ahv(j),'TextBackgroundColor','m');
                %set(ah,'HeadStyle','ellipse');
                %set(ah,'HeadSize', 3);
                uistack(ahv(j),'top');
                ox=x;
                oy=y;
                if ch=='s' | ch=='S'
                   ch='';
                end
                if  ch=='n' | ch=='N'
                    k=1;
                else
                    k=0;
                end
                while k==0
                    set(gcf, 'WindowButtonMotionFcn', @mouseMove);
                    k=waitforbuttonpress;
                    C=get(gca, 'CurrentPoint');
                    if not(k==0)
                        ch=get(gcf, 'CurrentCharacter');
                    end
                    if ch=='s' | ch=='S' | ch=='n' | ch=='N'
                        k=1;
                        x=ox;
                        y=oy;
                    else 
                        x(1)=C(1,1);
                        y(1)=C(1,2);
                    
                        [xg, yg] = ds2nfu(hax(1), x, y);
                        akv=[akv annotation('textarrow',xg ,yg ,'String', cur_label)];
                        set(akv(end),'TextBackgroundColor','c');
                        %set(akv(end),'HeadStyle','ellipse');
                        %set(akv(end),'HeadSize', 3);
                    end
                end
                C=get(gca, 'CurrentPoint');
                %x(1)=C(1,1);
                %y(1)=C(1,2);
                delete(ahv(j));
                delete(akv);
                [xg, yg] = ds2nfu(hax(1), x, y);
                ahv(j)=annotation('textarrow',xg ,yg ,'String', cur_label);
                set(ahv(j),'TextBackgroundColor','y');
                set(ahv(j),'Parent', hax(1));  % associate the textarrow annotation to the current axes;
                %[x, y] = nfu2ds(hax(1), xg, yg);
                set(ahv(j),'X',x); % the location in data units
                set(ahv(j),'Y',y); % the location in data units
                %axes('position', [200, 10, 205, 12]);
                %axes('position', [x(1), y(1), x(2), y(2)]);                
                %imshow(im);
                %set(ah,'HeadStyle','ellipse');
                set(ahv(j),'HeadStyle','none');
                set(ahv(j),'HeadSize', 3);
                if DAVINCI_ARROWHEADS
                    set(ahv(j),'HeadStyle','none');
                    %% quiver does not fit very well the purpose as arrow head size is proportional to length of arrow
                    %q=quiver(x(1), y(1), x(2)-x(1), y(2)-y(1), 0); %draw arrow to line
                    %q.Color=[0 0 0];
                    AH_MAX=((x(1)-x(2))^2 + (y(1)-y(2))^2)^(1/2);
                    ahl=2.0;
                    ahw=2.0;
                    if (ahl > AH_MAX)
                        ahl=AH_MAX;
                        ahw=ahw/ahl*AH_MAX;
                    end
                    %% DaVinci is much better - license to use arrows is free
                    davinci( 'arrow', 'X', x, 'Y', y, 'Head.Length', ahl, 'Head.Width', ahw);    % Draw an arrow.
                    %axis(axis);
                    %ar=arrow([x(1), y(1)],[x(2),y(2)]);    % Draw an arrow.
                    uistack(ah,'top');
                    %uistack(ar,'top');
                end
                if ZOOMABLE_TEXT_LABELS
                    im = image(str2im(ah.String, [1 0 0 5], 'back', 'y'));
                    im.XData=[x(1)-10 x(1)];
                    im.YData=[y(1)+3.0 y(1)];
                    delete(ahv(j));
                end
            end                   
            %delete(textfit_h);
        else
            %q=quiver(textfit_h_mat(1,:), textfit_h_mat(2,:), Lx - textfit_h_mat(1,:), 90- textfit_h_mat(2,:), 0); %draw arrow to line
            %q.Color=[0 0 0];
        end
    title(gca,'');    
    end
    
    %Nolegend labels placement
    nolegend_h={};
    if NOLEGEND_LABEL_PLACEMENT
        nolegend_h=nolegend(legend_lines, VLines_fields);
        %uistack(nolegend_h,'top');
    end
    
    %%re-plot cpu line
    axes(hax(1));
    set(gca,'color','w');
    delete(a);
    axes(hax(2));
    hold on;
    set(gca,'color','none');
    line_cpu=plot(hax(2),chart.Charts(cn).TimestampsCpu, chart.Charts(cn).Cpu, '.-', 'color', 'blue', 'LineWidth', 2, 'MarkerEdgeColor', 'blue', 'MarkerFaceColor', 'blue','MarkerSize', 12);
    xd=get(line_cpu,'xdata');
    yd=get(line_cpu,'ydata');
    a=area(xd, yd);
    set(a, 'FaceAlpha',0.2,'FaceColor', LightSteelBlue);
    %uistack(a, 'top');
    %axes(hax(1));
    if ZOOM_CONTROL
        h = zoom;
        %h.Motion = 'horizontal';
        h.ActionPreCallback = @myprecallback;
        h.ActionPostCallback = @mypostcallback;
        h.Enable = 'on';
    end
    %delete(ahv);
    origLim = [hax(1).XLim hax(1).YLim];
    origZoomLevel = 1.0;
    s.o = origLim;
    s.d = textfit_h_mat;
    s.lx = Lx;
    s.ly = Ly;
    s.ln = Ln;
    fig.UserData = s;
    %msgbox(sprintf('The original X-Limits are [%.2f %.2f].', origLim));
    zoom(1);


end