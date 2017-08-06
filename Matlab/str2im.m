function [I,A,H,W] = str2im(str,pad,varargin)
%Convert text string to rgb image
% str2im        - display example text
% str2im(str)      - text as char array or cellstr
% str2im(str,pad)     - margin: [N] or [H V] or [L R T B] as pixels or nan
% str2im(...,props)      - text property as value pairs, doc text
% I = str2im(..)         - returns rgb image data as uint8
% [I,A] = str2im(..)     - returns background alpha channel as uint8
% [I,A,H,W] = str2im(..) - returns image height and width
% 
%Remarks:
%-If a margin is nan then the background is cropped up to the text.
%-A bit slow because nothing is cached, a figure is generated for each
% call and getframe is used, usually take more then 0.03s to run.
%-Windows start menu may occlude text if it is not at bottom of screen or
% greater then 60 pixels in height.
%-Maximum image size is limited by screen resolution: [V_rez-60 H_rez].
% 
%Useful text properties:
% BackgroundColor     - {w} y m c r g b k [r g b]
% Color               - {k} y m c r g b w [r g b]
% FontSize            - {20} positive number
% FontUnits           - {points} pixels normalized inches centimeters
% FontWeight          - {normal} bold light demi
% FontAngle           - {normal} italic oblique
% FontName            - Helvetica FixedWidth ... (see listfonts)
% Interpreter         - {none} tex latex
% HorizontalAlignment - {left} center right
% 
%System defaults:
% get(0,{'defaultTextFontName' 'FixedWidthFontName'})
% 
%Example:
% figure(1),clf, str2im({'str' '2im'},nan,'back','y','fontw','b','hor','c')
% figure(2),clf, str2im('\phi_k^\pi',[0 0 -22 -10],'fonts',40,'interp','t')
% figure(3),clf, str2im('$$\int_0^2x^2\sin(x)dx$$','col','b','interp','l')
% 
%Example: burn text into image
% [T,A,H,W]=str2im(datestr(now),'fontn','FixedWidth','col','y','back',[.2 .2 .2]);
% figure(4),clf,I=imread('peppers.png');I(11:10+H,11:10+W,:)=I(1:H,1:W,:)+T;imshow(I)
% 
%Example: find fixed-width fonts
% f=listfonts,f(cellfun(@(f)numel(str2im('A','fontn',f))==numel(str2im('.','fontn',f)),f))
% 
%Example: show font browser gui
% figure(6),clf,set(gcf,'tool','fig')
% s = uicontrol('sty','edi','pos',[  5 5 150 50],'str',char(reshape(32:127,[],6)'),'max',1000);
% n = uicontrol('sty','pop','pos',[155 5 150 20],'str',['FixedWidth';sort(listfonts)]);
% w = uicontrol('sty','pop','pos',[305 5  60 20],'str',{'normal' 'bold' 'light' 'demi'});
% a = uicontrol('sty','pop','pos',[365 5  60 20],'str',{'normal' 'italic' 'oblique'});
% c = uicontrol('sty','pop','pos',[425 5  40 20],'str',{'k' 'y' 'm' 'c' 'r' 'g' 'b' 'w'});
% b = uicontrol('sty','pop','pos',[465 5  40 20],'str',{'w' 'y' 'm' 'c' 'r' 'g' 'b' 'k'});
% z = uicontrol('sty','pop','pos',[505 5  40 20],'str',cellstr(num2str((1:200)')),'val',20);
% F = @(v,i)v{i}; F = @(h)F(get(h,'str'),get(h,'val')); %function to get popup str
% set([n w a c b z],'call',@(x,y)str2im(get(s,'str'),'fontn',F(n),'fontw',...
%     F(w),'fonta',F(a),'color',F(c),'back',F(b),'fonts',str2double(F(z))));
% 
%See also: text, getframe, listfonts, rgb2gray, im2double
 
%Serge 2016, for corrections or suggestions email: s3rg3y@hotmail.com
%Tested on R2010b Win, R2012a Win, more testing feedback is desired
 
%defaults
if nargin<1 || isempty(str), str = 'Abc'; end
if nargin<2 || isempty(pad), pad = 0; end %pad amount
 
%checks
if ischar(pad), varargin = [pad varargin]; pad = 0; end %can skip pad argument
if numel(pad)==1, pad = pad*[1 1 1 1]; end %same pad for all sides
if numel(pad)==2, pad = [pad;pad]; end %same pad for L&R and T&B
crop = ~isfinite(pad(:));
pad(crop) = 0;
if isnumeric(str),str = char(str); end
 
fig = figure('unit','pix','color',[1 1 1],'menu','n','windowsty','modal'); %init figure, keep it on top 
axe = axes('par',fig,'pos',[0 0 1 1],'vis','off'); %axis to span entire figure
try %text properties and pad size might be invalid
    %Replaced obsolete 'fonts' with 'FontSize' reducedfrom 16 to 14 - Davide Bertolotti 2017
    txt = text('par',axe,'interp','n','FontSize',14,'str',str,varargin{:},'unit','pix'); %display text
    ext = get(txt,'extent'); %text bounding box
    set(txt,'pos',[3-ext(1)+pad(1) 2-ext(2)+pad(4) 0]) %(not sure why/if 3,2 are needed)
    W = ext(3)+pad(1)+pad(2)+2; %text width (not sure why/if 2 is needed)
    H = ext(4)+pad(3)+pad(4)+2; %text height
    set(fig,'pos',[1 60 W H]) %text must fit in figure & figure must be on-top and fit in screen
catch e
    close(fig) %close windows
    rethrow(e) %display error
end
if any(pad(:)>0) && ~isequal(get(txt,'back'),'none') %figure is visible and background color is set
    set(fig,'color',get(txt,'back')) %make figure same color as background
end
I = frame2im(getframe(fig,[1 1 W H])); %capture rgb image
if nargout>1 %generate alpha if needed
    set(txt,'color','k','back','w'), set(fig,'color','w') %change to black on white
    A = rgb2gray(255-frame2im(getframe(fig,[1 1 W H]))); %background alpha channel
end
if size(I,2)~=W || size(I,1)~=H
    %Suppress warnings - Davide Bertolotti
    %warning('str2im:TextLargerThenScreen','Text image was cropped because it did not fit on screen.')
end
if any(crop) %crop image
    [H,W,~] = size(I);
    t = any(any(abs(diff(I,[],1)),3),1); %find solid color rows
    if crop(1) && any(t), L = max(find(diff([0 t]),1,'first')-1,1); else L = 1; end
    if crop(2) && any(t), R = min(find(diff([t 0]),1,'last' )+1,W); else R = W; end
    t = any(any(abs(diff(I,[],2)),3),2); %find solid color columns
    if crop(3) && any(t), T = max(find(diff([0;t]),1,'first')-1,1); else T = 1; end
    if crop(4) && any(t), B = min(find(diff([t;0]),1,'last' )+1,H); else B = H; end
    I = I(T:B,L:R,:); %crop image
    if nargout>1
        A = A(T:B,L:R,:); %crop alpha also
        [H,W,~] = size(I);
    end
end
close(fig) %close figure
if ~nargout %plot results instead of returning image
    imshow(I)
    clear I
end