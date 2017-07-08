function[varargout] = nolegend(varargin)
%labels the lines in a graph (instead of using legend) as suggested by Tufte
%nolegend(labels)
%nolegend(h,labels)
%nolegend(labels,OPTIONS)
%nolegend(h,labels,OPTIONS)
%
%h      = the handles for each line - numeric (if not entered, default is 
%         get(gca,'Children')
%labels = the labels to output - cell of strings.  it must be entered
%OPTIONS= a structure containing data like SpacingPct, XOffsetPct,
%         Position, more info provided below.  This uses the input parser,
%         so options could also be entered as PropertyName, PropertyValue
%
%example code
%{
t = 0:.1:19.6;
rr = 3; cc = 1;
subplot(rr,cc,1)
plot(t,sin(t),t,cos(t))
nolegend({'sin','cos'});

subplot(rr,cc,2)
plot(t,sin(t),t,cos(t))
nolegend({'sin','cos'},'SpacingPct',.20);

subplot(rr,cc,3)
plot(t,sin(t),t,cos(t))
opt = [];
nolegend({'sin','cos'},'Position','left');

for i=1:rr
    subplot(rr,cc,i)
    ylim([-1.5 1.5])
end
%}
 

%Default Parameters    
    %TextProperties = [];
    o.SpacingPct     = .05; %minimum spacing as a fraction of the Y-size of the axis
    o.XOffsetPct     = .02; %X offset as a fraction of the size of the X axis
    o.Position       = 'right'; %could be left or right       
    o.XLimTweak      =   1; %Tweak the XLim a bit so text is more likely to be in plot
    o.XLimTweakPct   =  .1;
             
%parse the inputs
    %line handles
    h = findobj(gca,'Type','Line');
    h = flipud(fliplr(h));
    if ~isempty(varargin)
        if ishandle(varargin{1})
            h = varargin{1};
            varargin(1) = [];
        end
    end
    
    %line labels
    labels = get(h,'DisplayName');
    if ~isempty(varargin)
        if iscell(varargin{1})
            labels = varargin{1};
            varargin(1) = [];
        end
    end 
    
    %Put default parameter names into a variable and setup input parser
    if ~isempty(varargin)
        p = inputParser;
        fo = fieldnames(o);
        for i=1:length(fo)
            p.addOptional(fo{i},o.(fo{i}));
        end
        if length(varargin)==1 %the options are a structure
            p.parse(varargin{1});
        else
            p.parse(varargin{:})
        end
        o = p.Results;
    end
    
%check if we're in linear space or logspace
%calculate the minimum Y spacing between each label
    YScale = get(gca,'YScale');
    XLim   = get(gca,'XLim');
    YLim   = get(gca,'YLim');
    
    switch YScale
        case {'linear'}
            dY = o.SpacingPct*(YLim(2)-YLim(1));
        case {'log'}
            dY = o.SpacingPct*(log10(YLim(2)) - log10(YLim(1)));
    end
    dX = o.XOffsetPct*(XLim(2)-XLim(1));
        
%first get the y position of each line
    xpos  = zeros(size(h));
    ypos  = zeros(size(h));
    col   = cell(size(h));
    for i=1:length(h)
        x = get(h(i),'XData');
        y = get(h(i),'YData');
        col{i} = get(h(i),'Color');
        switch o.Position
            case 'left'
                [xpos(i), ind] = min(x);
                xpos(i)       = xpos(i)-dX;
                ypos(i)       = y(ind);
            case 'right'
                [xpos(i), ind]= max(x);
                xpos(i)       = xpos(i)+dX;
                ypos(i)       = y(ind);
            otherwise
                error('invalid Position');
        end
    end
    
    if strcmp(YScale,'log')
        ypos = log10(ypos);
    end
    
%sort by position, starting at bottom first
    [ysort, isort] = sort(ypos);
    labelsort     = labels(isort);
    xsort         = xpos(isort);
    csort         = col(isort);
            
%calculate new y positions (called z)     
    zsort = zeros(size(ysort));
    for i=1:length(ysort)        
        if i==1
            zsort(i) = ysort(i);
        else
            if ysort(i)-zsort(i-1) < dY
                zsort(i) = zsort(i-1) + dY;
            else
                zsort(i) = ysort(i);
            end
        end
    end
    
    if strcmp(YScale,'log')
        zsort = 10.^(zsort);
    end
    
%put the labels in the graph
    htxt = zeros(size(h));
    for i=1:length(zsort)
        htxt(i) = text(xsort(i),zsort(i),labelsort{i},'Color',csort{i});
        if strcmp(o.Position,'left')
            set(htxt(i),'HorizontalAlignment','Right');
        end            
    end
    
%tweak the XLimits
    if o.XLimTweak==1
        XLim = get(gca,'XLim');
        switch o.Position
            case {'right'}
                xx = max(x) + (max(x)-min(x))*o.XLimTweakPct;
                xlim([XLim(1) max(xx,XLim(2))]);
            case {'left'}
                xx = min(x) - (max(x)-min(x))*o.XLimTweakPct;
                xlim([min(xx,XLim(1)) XLim(2)]);
        end
    end
    
%outputs
    varargout{1} = htxt;
