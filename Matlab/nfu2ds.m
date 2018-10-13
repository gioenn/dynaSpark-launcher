function varargout = nfu2ds(varargin)
% NFU2DS function Converts normalized figure units into data space units, 
% using the current axes. 
% It is a partial implementation of the inverse function of function DS2NFU
% originally developed by:  Michelle Hirsch
%                           mhirsch@mathworks.com
%                           Copyright 2006-2014 The MathWorks, Inc
%
% [X, Y] = NFU2DS(Xf, Yf) converts Xf,Yf coordinates from
% normalized figure units to data space.  This is
% useful to retrieve posiion information from ANNOTATION.  
%
% [X, Y] = NFU2DS(HAX, Xf, Yf) converts Xf,Yf coordinates from
% normalized figure units to data space, on specified axes HAX.
%
% Davide Bertolotti
% davide.bertolotti@mail.polimi.it
% Copyright 2017
%% Process inputs
error(nargchk(1, 3, nargin))

% Determine if axes handle is specified
if length(varargin{1})== 1 && ishandle(varargin{1}) && strcmp(get(varargin{1},'type'),'axes')	
	hAx = varargin{1};
	varargin = varargin(2:end);
else
	hAx = gca;
end;

errmsg = ['Invalid input.  Coordinates must be specified as 1 four-element \n' ...
	'position vector or 2 equal length (x,y) vectors.'];

% Proceed with remaining inputs
if length(varargin)==1	% Must be 4 elt POS vector
	pos = varargin{1};
	if length(pos) ~=4, 
		error(errmsg);
	end;
else
	[x,y] = deal(varargin{:});
	if length(x) ~= length(y)
		error(errmsg)
	end
end

	
%% Get limits
axun = get(hAx,'Units');
set(hAx,'Units','normalized');
axpos = get(hAx,'Position');
axlim = axis(hAx);
axwidth = diff(axlim(1:2));
axheight = diff(axlim(3:4));


%% Transform data
if exist('x','var')
    %% Original transformation
	%varargout{1} = (x-axlim(1))*axpos(3)/axwidth + axpos(1);
	%varargout{2} = (y-axlim(3))*axpos(4)/axheight + axpos(2);
    
    %% Inverse transformation
    varargout{1} = (x-axpos(1))*axwidth/axpos(3)+axlim(1);
	varargout{2} = (y-axpos(2))*axheight/axpos(4)+axlim(3);
else
    %%Original transformation
    %pos(1) = (pos(1)-axlim(1))/axwidth*axpos(3) + axpos(1);
	%pos(2) = (pos(2)-axlim(3))/axheight*axpos(4) + axpos(2);
	%pos(3) = pos(3)*axpos(3)/axwidth;
	%pos(4) = pos(4)*axpos(4)/axheight;
	%varargout{1} = pos;
    
    %%Inverse transformation from one 4-elements position vector not implemented
    error('Transformation from one 4-elements position vector not implemented: please use 2 equal length (x,y) vectors');
end

%% Restore axes units
set(hAx,'Units',axun)

